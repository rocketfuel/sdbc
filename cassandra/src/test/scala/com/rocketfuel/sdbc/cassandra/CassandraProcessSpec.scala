package com.rocketfuel.sdbc.cassandra

import com.rocketfuel.sdbc.Cassandra._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import scalaz.stream._
import scalaz.concurrent.Task

class CassandraProcessSpec
  extends CassandraSuite
  with GeneratorDrivenPropertyChecks {

  override implicit val generatorDrivenConfig: PropertyCheckConfig = PropertyCheckConfig(maxSize = 10)

  test("values are inserted and selected") {implicit connection =>
    Query(s"CREATE TABLE $keyspace.tbl (id int PRIMARY KEY, x int)").execute()

    case class IdAndX(x: Int, id: Int)

    forAll { randomValues: Seq[Int] =>
      val randoms =
        randomValues.zipWithIndex.map(IdAndX.tupled)

      val insert: Process[Task, Unit] = {
        val randomStream = Process.emitAll(randoms)
        randomStream.to(Query(s"INSERT INTO $keyspace.tbl (id, x) VALUES (@id, @x)").productSink)
      }

      insert.run.run

      val select = Query[Int](s"SELECT x FROM $keyspace.tbl")

      val results = io.iterator(select.task.iterator()).runLog.run

      assertResult(randomValues.sorted)(results.sorted)

      truncate()
    }
  }

}
