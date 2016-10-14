package com.rocketfuel.sdbc.cassandra

import com.rocketfuel.sdbc.Cassandra._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import fs2.{Stream, Task}

class CassandraProcessSpec
  extends CassandraSuite
  with GeneratorDrivenPropertyChecks {

  implicit val strategy =
    fs2.Strategy.sequential

  override implicit val generatorDrivenConfig: PropertyCheckConfig = PropertyCheckConfig(maxSize = 10)

  test("values are inserted and selected") {implicit connection =>
    Query(s"CREATE TABLE $keyspace.tbl (id int PRIMARY KEY, x int)").execute()

    case class IdAndX(x: Int, id: Int)

    forAll { randomValues: Seq[Int] =>
      val randoms: Seq[IdAndX] =
        randomValues.zipWithIndex.map(IdAndX.tupled)

      val insert: Stream[Task, Unit] = {
        val randomStream = Stream[Task, IdAndX](randoms: _*)
        randomStream.to(Query(s"INSERT INTO $keyspace.tbl (id, x) VALUES (@id, @x)").sink[Task].product)
      }

      insert.run.unsafeRun()

      val select = Query[Int](s"SELECT x FROM $keyspace.tbl")

      val results = select.task.iterator().unsafeRun().toVector

      assertResult(randomValues.sorted)(results.sorted)

      truncate()
    }
  }

}
