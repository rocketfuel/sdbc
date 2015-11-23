package com.rocketfuel.sdbc.scalaz

import com.rocketfuel.sdbc.cassandra.datastax._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import scalaz.concurrent.Task
import _root_.scalaz.stream._

class DatastaxProcessSpec
  extends DatastaxSuite
  with GeneratorDrivenPropertyChecks {

  override implicit val generatorDrivenConfig: PropertyCheckConfig = PropertyCheckConfig(maxSize = 10)

  test("values are inserted and selected") {implicit connection =>
    Execute("CREATE KEYSPACE spc WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}").execute()
    Execute("CREATE TABLE spc.tbl (id int PRIMARY KEY, x int)").execute()

    forAll { (randoms: Seq[Int]) =>

      val insert: Process[Task, Unit] = {
        val execute = Execute("INSERT INTO spc.tbl (id, x) VALUES (@id, @x)")
        val randomsParameters = Process.emitAll(randoms.zipWithIndex).map[ParameterList](x => Seq("id" -> x._2, "x" -> x._1))
        randomsParameters.to(Process.datastax.params.execute(execute))
      }

      val select = Process.datastax.select(Select[Int]("SELECT x FROM spc.tbl"))

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
