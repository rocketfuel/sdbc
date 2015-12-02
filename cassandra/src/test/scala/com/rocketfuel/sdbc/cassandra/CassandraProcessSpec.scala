package com.rocketfuel.sdbc.cassandra

import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scalaz.stream._
import scalaz.concurrent.Task

class CassandraProcessSpec
  extends CassandraSuite
  with GeneratorDrivenPropertyChecks {

  override implicit val generatorDrivenConfig: PropertyCheckConfig = PropertyCheckConfig(maxSize = 10)

  test("values are inserted and selected") {implicit connection =>
    Execute("CREATE KEYSPACE spc WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}").execute()
    Execute("CREATE TABLE spc.tbl (id int PRIMARY KEY, x int)").execute()

    forAll { (randoms: Seq[Int]) =>

      val insert: Process[Task, Unit] = {
        val execute = Execute("INSERT INTO spc.tbl (id, x) VALUES (@id, @x)")
        val randomsParameters = Process.emitAll(randoms.zipWithIndex).map[ParameterList](x => Seq("id" -> x._2, "x" -> x._1))
        randomsParameters.to(Process.cassandra.params.execute(execute))
      }

      val select = Process.cassandra.select(Select[Int]("SELECT x FROM spc.tbl"))

      val combined = for {
        _ <- insert
        ints <- select
      } yield ints

      val results = combined.runLog.run

      assertResult(randoms.sorted)(results.sorted)

      RichResultSetSpec.truncate()
    }
  }

}
