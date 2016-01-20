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
    Query(s"CREATE TABLE $keyspace.tbl (id int PRIMARY KEY, x int)").io.execute()

    forAll { (randoms: Seq[Int]) =>

      val insert: Process[Task, Unit] = {
        val execute = s"INSERT INTO $keyspace.tbl (id, x) VALUES (@id, @x)"
        val randomStream = Process.emitAll(randoms.zipWithIndex).toSource
        randomStream.map { case (x, id) =>  Parameters("id" -> id, "x" -> x)}.through(Query.stream.ofParameters(execute)).map(Function.const(()))
      }

      val select = Query[Int](s"SELECT x FROM $keyspace.tbl").stream()

      val combined = for {
        _ <- insert
        ints <- select
      } yield ints

      val results = combined.runLog.run

      assertResult(randoms.sorted)(results.sorted)

      truncate()
    }
  }

}
