package com.rocketfuel.sdbc.postgresql

import org.scalatest.BeforeAndAfterEach
import com.rocketfuel.sdbc.PostgreSql._
import scala.collection.immutable.Seq

class RichResultSpec
  extends PostgreSqlSuite
  with BeforeAndAfterEach {

  test("option() selects nothing from an empty table") {implicit connection =>
    Ignore("CREATE TABLE tbl (x int)").execute()

    val result = Select[Int]("SELECT * FROM tbl").option()

    assert(result.isEmpty, "Selecting from an empty table yielded a row.")
  }

  test("option() selects something from a nonempty table") {implicit connection =>
    Ignore("CREATE TABLE tbl (x serial)").execute()
    Ignore("INSERT INTO tbl DEFAULT VALUES").execute()

    val result = Select[Int]("SELECT * FROM tbl").option()

    assert(result.isDefined, "Selecting from a table with a row did not yeild a row.")
  }

  test("seq() works on an empty result") {implicit connection =>
    Ignore("CREATE TABLE tbl (x serial)").execute()
    val results = Select[Int]("SELECT * FROM tbl").vector()
    assert(results.isEmpty)
  }

  test("seq() works on a single result") {implicit connection =>
    val results = Select[Int]("SELECT 1::integer").vector()
    assertResult(Vector(1))(results)
  }

  test("seq() works on several results") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt())
    Ignore("CREATE TABLE tbl (x int)").execute()

    val batch = randoms.foldLeft(Batch("INSERT INTO tbl (x) VALUES (@x)")) {
      case (batch, r) =>
        batch.add("x" -> r)
    }

    val insertions = batch.batch()

    assertResult(randoms.size)(insertions.sum[Long])

    val results = Select[Int]("SELECT x FROM tbl").vector()
    assertResult(randoms)(results)
  }

  test("using SelectForUpdate to update a value works") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt()).sorted

    Ignore("CREATE TABLE tbl (id serial PRIMARY KEY, x int)").execute()

    val incrementedRandoms = randoms.map(_+1)

    val batch = randoms.foldRight(Batch("INSERT INTO tbl (x) VALUES (@x)")) {
      case (r, batch) =>
        batch.add("x" -> r)
    }

    batch.execute()

    val rows = selectForUpdate"SELECT * FROM tbl".iterator()
    try {
      for (row <- rows) {
        row("x") = row[Option[Int]]("x").map(_ + 1)
        row.updateRow()
      }
    } finally rows.close()

    val incrementedFromDb = Select[Int]("SELECT x FROM tbl ORDER BY x ASC").vector()

    assert(incrementedFromDb.zip(incrementedRandoms).forall(xs => xs._1 == xs._2))
  }

  override protected def afterEach(): Unit = {
    withPg { implicit connection =>
      Ignore("DROP TABLE IF EXISTS tbl;").execute()
    }
  }

}
