package com.rocketfuel.sdbc.cassandra

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import fs2.{Stream, Task}
import scala.concurrent.ExecutionContext

class CassandraStreamSpec
  extends CassandraSuite
  with GeneratorDrivenPropertyChecks {

  implicit val strategy =
    fs2.Strategy.fromExecutionContext(ExecutionContext.global)

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

    val rowsPerKeyspace = util.Random.nextInt(50) + 50

    val rowCount = keyspaceCount * rowsPerKeyspace

    //There's the default test keyspace, so create keyspaceCount - 1 more.
    for (_ <- 0 until keyspaceCount - 1)
      createKeyspace()

    val expectedRows =
      for {
        id <- 0 until rowCount
      } yield TestTable(id, id + 1)

    val expectedRowsByKeyspace =
      for {
        (keyspace, keyspaceRows) <- keyspaces.zip(expectedRows.sliding(rowsPerKeyspace, rowsPerKeyspace).toSet).toMap
      } yield keyspace -> keyspaceRows

    val insertKeys =
      for {
        (keyspace, keyspaceRows) <- expectedRowsByKeyspace
        insert <- keyspaceRows.map(_.insert(keyspace))
      } yield insert

    for (keyspace <- keyspaces) {
      implicit val session = client.connect(keyspace)
      TestTable.createTable.execute()
      session.close()
    }

    fs2.concurrent.join(keyspaceCount)(Stream(insertKeys.toSeq: _*).
      through(QueryableWithKeyspace.pipe[Task, TestTable.Insert, Unit])
    ).run.unsafeRun()

    val results =
      fs2.concurrent.join(keyspaceCount)(
        Stream.constant(TestTable.All).zip(Stream(keyspaces: _*)).
          through(Queryable.pipeWithKeyspace[Task, TestTable.All.type, TestTable])
      ).runLog.unsafeRun()

    assertResult(rowCount)(results.size)

    assertResult(expectedRows.toSet)(results.toSet)
  }

  case class TestTable(id: Int, value: Int) {
    def insert(keyspace: String): TestTable.Insert =
      TestTable.Insert(keyspace, id, value)
  }

  object TestTable {

    val createTable =
      Query[Unit]("CREATE TABLE tbl (id int PRIMARY KEY, value int)")

    object All {
      implicit val queryable: Queryable[All.type, TestTable] = {
        Query[TestTable]("SELECT id, value FROM tbl").queryable[All.type].constant
      }
    }

    case class Insert(keyspace: String, id: Int, value: Int)

    object Insert {
      implicit val keyspaceQueryable: QueryableWithKeyspace[Insert, Unit] = {
        Query[Unit]("INSERT INTO tbl (id, value) VALUES (@id, @value)").
          queryable[Insert].product.withKeyspace(_.keyspace)
      }
    }

  }

}
