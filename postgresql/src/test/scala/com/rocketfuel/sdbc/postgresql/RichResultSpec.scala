package com.rocketfuel.sdbc.postgresql

import org.scalatest.BeforeAndAfterEach
import com.rocketfuel.sdbc.PostgreSql._
import scala.collection.immutable.Seq

class RichResultSpec
  extends PostgreSqlSuite
  with BeforeAndAfterEach {

  test("option() selects nothing from an empty table") {implicit connection =>
    Select[Unit]("CREATE TABLE tbl (x int)").run()

    val result = Select[Option[Int]]("SELECT * FROM tbl").run()

    assert(result.isEmpty, "Selecting from an empty table yielded a row.")
  }

  test("option() selects something from a nonempty table") {implicit connection =>
    Select[Unit]("CREATE TABLE tbl (x serial)").run()
    Select[Unit]("INSERT INTO tbl DEFAULT VALUES").run()

    val result = Select[Option[Int]]("SELECT * FROM tbl").run()

    assert(result.isDefined, "Selecting from a table with a row did not yeild a row.")
  }

  test("seq() works on an empty result") {implicit connection =>
    Select[Unit]("CREATE TABLE tbl (x serial)").run()
    val results = Select[Vector[Int]]("SELECT * FROM tbl").run()
    assert(results.isEmpty)
  }

  test("seq() works on a single result") {implicit connection =>
    val results = Select[Vector[Int]]("SELECT 1::integer").run()
    assert(results == Seq(1))
  }

  test("seq() works on several results") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt())
    Select[Unit]("CREATE TABLE tbl (x int)").run()

    val batch = randoms.foldLeft(Batch("INSERT INTO tbl (x) VALUES (@x)")) {
      case (batch, r) =>
        batch.add("x" -> r)
    }

    val insertions = batch.run()

    assert(insertions.sum[Long] == randoms.size)

    val results = Select[Vector[Int]]("SELECT x FROM tbl").run()
    assert(results == randoms)
  }

  test("using SelectForUpdate to update a value works") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt()).sorted

    Select[Unit]("CREATE TABLE tbl (id serial PRIMARY KEY, x int)").run()

    val incrementedRandoms = randoms.map(_+1)

    val batch = randoms.foldRight(Batch("INSERT INTO tbl (x) VALUES (@x)")) {
      case (r, batch) =>
        batch.add("x" -> r)
    }

    batch.run()

    for(row <- selectForUpdate"SELECT * FROM tbl".run()) {
      row("x") = row[Option[Int]]("x").map(_ + 1)
      row.updateRow()
    }

    val incrementedFromDb = Select[Vector[Int]]("SELECT x FROM tbl ORDER BY x ASC").run()

    assert(incrementedFromDb.zip(incrementedRandoms).forall(xs => xs._1 == xs._2))
  }

  override protected def afterEach(): Unit = {
    withPg { implicit connection =>
      Select[Unit]("DROP TABLE IF EXISTS tbl;").run()
    }
  }

}
