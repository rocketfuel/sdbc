package com.rocketfuel.sdbc.cassandra

import com.rocketfuel.sdbc.Cassandra._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import fs2.{Stream, Task}
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class CassandraStreamSpec
  extends CassandraSuite
  with GeneratorDrivenPropertyChecks {

  implicit val strategy =
    fs2.Strategy.sequential

  override implicit val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(sizeRange = 10)

  test("values are inserted and selected") {implicit connection =>
    Query.execute(s"CREATE TABLE $keyspace.tbl (id int PRIMARY KEY, x int)")

    case class IdAndX(id: Int, x: Int)

    forAll { randomValues: Seq[Int] =>
      val randoms: Seq[IdAndX] =
        randomValues.zipWithIndex.map(t => IdAndX.tupled(t.swap))

      val insert: Stream[Task, Unit] = {
        val randomStream = Stream[Task, IdAndX](randoms: _*)
        randomStream.to(Query(s"INSERT INTO $keyspace.tbl (id, x) VALUES (@id, @x)").sink[Task].product)
      }

      insert.run.unsafeRun()

      val select = Query[Int](s"SELECT x FROM $keyspace.tbl")

      val results = select.iterator().toVector

      assertResult(randomValues.sorted)(results.sorted)

      truncate(tableName = "tbl")
    }
  }

  test("can stream from multiple keyspaces") {_ =>

    val keyspaceCount = util.Random.nextInt(5) + 3

    val rowCount = util.Random.nextInt(50) + 50

    //There's the default test keyspace, so create keyspaceCount - 1 more.
    for (_ <- 0 until keyspaceCount - 1)
      createRandomKeyspace()

    val expectedRows = 0 until rowCount map(id => TestTable(id, id + 1))

    for (keyspace <- keyspaces) {
      implicit val session = client.connect(keyspace)
      TestTable.create.execute()
      session.close()
    }

    val rowsWithKeyspace =
      for {
        keyspace <- Stream(keyspaces: _*)
        keyspaceRow <- Stream(expectedRows: _*).map(row => (keyspace, row.toInsert))
      } yield keyspaceRow

    rowsWithKeyspace.
      through(Queryable.pipeWithKeyspace[Task, TestTable.Insert, Unit]).
      flatMap(identity).run.unsafeRun()

    val resultsFuture =
      Stream(keyspaces: _*).zip(Stream.constant(TestTable.All)).
        through(Queryable.pipeWithKeyspace[Task, TestTable.All.type, TestTable]).
        flatMap(identity).
        runLog.unsafeRunAsyncFuture()

    val actualResults =
      Await.result(resultsFuture, Duration.Inf)

    assertResult(keyspaceCount * rowCount)(actualResults.size)

    assertResult(expectedRows.toSet)(actualResults.toSet)
  }

  case class TestTable(id: Int, value: Int) {
    def toInsert: TestTable.Insert =
      TestTable.Insert(id, value)
  }

  object TestTable {

    val create =
      Query[Unit]("CREATE TABLE tbl (id int PRIMARY KEY, value int)")

    case object All

    implicit val queryable: Queryable[All.type, TestTable] = {
      val query = Query[TestTable]("SELECT id, value FROM tbl")
      Queryable[All.type, TestTable](Function.const(query))
    }

    case class Insert(id: Int, value: Int)

    object Insert {
      implicit val queryable: Queryable[Insert, Unit] = {
        val query = Query[Unit]("INSERT INTO tbl (id, value) VALUES (@id, @value)")
        Queryable[Insert, Unit](query.onProduct(_))
      }
    }

    case class Id(id: Int)

    object Id {
      implicit val queryable: Queryable[Id, TestTable] = {
        val query = Query[TestTable]("SELECT id, value FROM tbl WHERE id = @id")
        Queryable[Id, TestTable](id => query.on("id" -> id.id))
      }
    }

  }


}
