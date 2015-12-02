package com.rocketfuel.sdbc.postgresql.jdbc

import com.rocketfuel.sdbc.PostgreSql
import org.scalatest.BeforeAndAfterEach
import PostgreSql._
import scala.collection.immutable.Seq

class RichResultSpec
  extends PostgreSqlSuite
  with BeforeAndAfterEach {

  test("option() selects nothing from an empty table") {implicit connection =>
    Execute("CREATE TABLE tbl (x int)").execute()

    val result = Select[Int]("SELECT * FROM tbl").option()

    assert(result.isEmpty, "Selecting from an empty table yielded a row.")
  }

  test("option() selects something from a nonempty table") {implicit connection =>
    Execute("CREATE TABLE tbl (x serial)").execute()
    Execute("INSERT INTO tbl DEFAULT VALUES").execute()

    val result = Select[Int]("SELECT * FROM tbl").option()

    assert(result.isDefined, "Selecting from a table with a row did not yeild a row.")
  }

  test("seq() works on an empty result") {implicit connection =>
    Execute("CREATE TABLE tbl (x serial)").execute()
    val results = Select[Int]("SELECT * FROM tbl").iterator().toSeq
    assert(results.isEmpty)
  }

  test("seq() works on a single result") {implicit connection =>
    val results = Select[Int]("SELECT 1::integer").iterator().toSeq
    assert(results == Seq(1))
  }

  test("seq() works on several results") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt())
    Execute("CREATE TABLE tbl (x int)").execute()

    val batch = randoms.foldLeft(Batch("INSERT INTO tbl (x) VALUES (@x)")) {
      case (batch, r) =>
        batch.addBatch("x" -> r)
    }

    val insertions = batch.iterator()

    assert(insertions.sum[Long] == randoms.size)

    val results = Select[Int]("SELECT x FROM tbl").iterator().toSeq
    assert(results == randoms)
  }

  test("using SelectForUpdate to update a value works") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt()).sorted

    Execute("CREATE TABLE tbl (id serial PRIMARY KEY, x int)").execute()

    val incrementedRandoms = randoms.map(_+1)

    val batch = randoms.foldRight(Batch("INSERT INTO tbl (x) VALUES (@x)")) {
      case (r, batch) =>
        batch.addBatch("x" -> r)
    }

    batch.iterator()

    for(row <- connection.iteratorForUpdate("SELECT * FROM tbl")) {
      row("x") = row.get[Int]("x").map(_ + 1)
      row.updateRow()
    }

    val incrementedFromDb = connection.iterator[Int]("SELECT x FROM tbl ORDER BY x ASC").toSeq

    assert(incrementedFromDb.zip(incrementedRandoms).forall(xs => xs._1 == xs._2))
  }

  override protected def afterEach(): Unit = {
    withPg(_.execute("DROP TABLE IF EXISTS tbl;"))
  }

}
