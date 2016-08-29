package com.rocketfuel.sdbc.postgresql

import org.scalatest.BeforeAndAfterEach
import com.rocketfuel.sdbc.PostgreSql._
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
    val results = Select[Int]("SELECT * FROM tbl").iterator().toVector
    assert(results.isEmpty)
  }

  test("seq() works on a single result") {implicit connection =>
    val results = Select[Int]("SELECT 1::integer").iterator().toVector
    assertResult(Vector(1))(results)
  }

  test("seq() works on several results") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt())
    Execute("CREATE TABLE tbl (x int)").execute()

    val batch = randoms.foldLeft(Batch("INSERT INTO tbl (x) VALUES (@x)")) {
      case (batch, r) =>
        batch.add("x" -> r)
    }

    val insertions = batch.run()

    assertResult(randoms.size)(insertions.sum[Long])

    val results = Select[Int]("SELECT x FROM tbl").iterator().toVector
    assertResult(randoms)(results)
  }

  test("using SelectForUpdate to update a value works") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt()).sorted

    Execute("CREATE TABLE tbl (id serial PRIMARY KEY, x int)").execute()

    val incrementedRandoms = randoms.map(_+1)

    val batch = randoms.foldRight(Batch("INSERT INTO tbl (x) VALUES (@x)")) {
      case (r, batch) =>
        batch.add("x" -> r)
    }

    batch.execute()

    for(row <- selectForUpdate"SELECT * FROM tbl".iterator()) {
      row("x") = row[Option[Int]]("x").map(_ + 1)
      row.updateRow()
    }

    val incrementedFromDb = Select[Int]("SELECT x FROM tbl ORDER BY x ASC").iterator().toVector

    assert(incrementedFromDb.zip(incrementedRandoms).forall(xs => xs._1 == xs._2))
  }

  override protected def afterEach(): Unit = {
    withPg { implicit connection =>
      Execute("DROP TABLE IF EXISTS tbl;").execute()
    }
  }

}
