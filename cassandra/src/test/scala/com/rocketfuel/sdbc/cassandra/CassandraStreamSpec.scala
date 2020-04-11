package com.rocketfuel.sdbc.cassandra

import cats.effect.IO
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import fs2.Stream

class CassandraStreamSpec
  extends CassandraSuite
  with GeneratorDrivenPropertyChecks {

  override implicit val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(sizeRange = 10)

  test("values are inserted and selected") {implicit connection =>
    Query.execute("CREATE TABLE tbl (id int PRIMARY KEY, x int)")

    case class IdAndX(id: Int, x: Int)

    forAll { randomValues: Seq[Int] =>
      val randoms: Seq[IdAndX] =
        randomValues.zipWithIndex.map(t => IdAndX.tupled(t.swap))

      val insert: Stream[IO, Unit] = {
        val randomStream = Stream[IO, IdAndX](randoms: _*)
        randomStream.through(Query("INSERT INTO tbl (id, x) VALUES (@id, @x)").sink[IO].product)
      }

      insert.compile.drain.unsafeRunSync()

      val select = Query[Int]("SELECT x FROM tbl")

      val results = select.iterator().toVector

      assertResult(randomValues.sorted)(results.sorted)

      truncate(tableName = "tbl")
    }
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
      implicit val keyspaceQueryable: Queryable[Insert, Unit] = {
        Query[Unit]("INSERT INTO tbl (id, value) VALUES (@id, @value)").
          queryable[Insert].product
      }
    }

  }

}
