package com.rocketfuel.sdbc.cassandra

import com.rocketfuel.sdbc.Cassandra._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import fs2.{Stream, Task}

class CassandraStreamSpec
  extends CassandraSuite
  with GeneratorDrivenPropertyChecks {

  implicit val strategy =
    fs2.Strategy.sequential

  override implicit val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(sizeRange = 10)

  test("values are inserted and selected") {implicit connection =>
    Query(s"CREATE TABLE $keyspace.tbl (id int PRIMARY KEY, x int)").execute()

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

      truncate()
    }
  }

  test("can stream from multiple keyspaces") {_ =>

    randomKeyspace()

    val values = 0 until 100 map(TestTable.Value(_))

    //insert
    for (keyspace <- keyspaces) {
      implicit val session = client.connect(keyspace)
      TestTable.create.execute()
      Stream(values: _*).covary[Task].through(Queryable.streams[Task, TestTable.Value, Unit]).flatMap(identity).run.unsafeValue()
      session.close()
    }

    val keys =
      for {
        keyspace <- keyspaces
      } yield (keyspace, TestTable.All)

    val results =
      Stream(keys: _*).through(Queryable.streamsWithKeyspace[Task, TestTable.All.type, TestTable]).flatMap(identity).runLog.unsafeValue().get

    assertResult(300)(results.size)
  }

  case class TestTable(id: Int, value: Int) {

  }

  object TestTable {

    val create =
      Query[Unit]("CREATE TABLE TestTable (id int PRIMARY KEY, value int)")

    case object All

    implicit val queryable: Queryable[All.type, TestTable] =
      Queryable[All.type, TestTable](_ => Query[TestTable]("SELECT id, value FROM TestTable"))

    case class Value(value: Int)

    object Value {
      implicit val queryable: Queryable[Value, Unit] =
        Queryable[Value, Unit](v => Query[Unit]("INSERT INTO TestTable (value) VALUES (@value)").on("value" -> v.value))
    }

    case class Id(id: Int)

    object Id {
      implicit val queryable: Queryable[Id, TestTable] =
        Queryable[Id, TestTable](id => Query[TestTable]("SELECT id, value FROM TestTable WHERE id = @id").on("id" -> id.id))
    }

  }


}
