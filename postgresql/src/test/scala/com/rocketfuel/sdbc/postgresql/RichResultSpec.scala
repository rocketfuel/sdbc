package com.rocketfuel.sdbc.postgresql

import org.scalatest.BeforeAndAfterEach
import scala.collection.immutable.Seq

class RichResultSpec
  extends PostgreSqlSuite.Base
  with BeforeAndAfterEach {

  test("option() selects nothing from an empty table") {implicit connection =>
    Ignore("CREATE TABLE tbl (x int)").ignore()

    val result = Select[Int]("SELECT * FROM tbl").option()

    assert(result.isEmpty, "Selecting from an empty table yielded a row.")
  }

  test("option() selects something from a nonempty table") {implicit connection =>
    Ignore("CREATE TABLE tbl (x serial)").ignore()
    Ignore("INSERT INTO tbl DEFAULT VALUES").ignore()

    val result = Select[Int]("SELECT * FROM tbl").option()

    assert(result.isDefined, "Selecting from a table with a row did not yeild a row.")
  }

  test("seq() works on an empty result") {implicit connection =>
    Ignore("CREATE TABLE tbl (x serial)").ignore()
    val results = Select[Int]("SELECT * FROM tbl").vector()
    assert(results.isEmpty)
  }

  test("seq() works on a single result") {implicit connection =>
    val results = Select[Int]("SELECT 1::integer").vector()
    assertResult(Vector(1))(results)
  }

  test("seq() works on several results") {implicit connection =>
    val randoms = Vector.fill(10)(util.Random.nextInt())
    Ignore("CREATE TABLE tbl (x int)").ignore()

    val insert = Insert("INSERT INTO tbl (x) VALUES (@x)")

    val batch = Batch(randoms.map(r => insert.on("x" -> r)))

    val insertions = batch.batch()

    assertResult(randoms.size)(insertions.sum)

    val results = Select[Int]("SELECT x FROM tbl").vector()
    assertResult(randoms)(results)
  }

  test("using SelectForUpdate to update a value works") {implicit connection =>
    val batchCount = 10

    val randoms = Vector.fill(batchCount)(util.Random.nextInt()).sorted

    Ignore.ignore("CREATE TABLE tbl (id serial PRIMARY KEY, x int)")

    val incrementedRandoms = randoms.map(_+1)

    val insert = Insert("INSERT INTO tbl (x) VALUES (@x)")

    val batch = Batch(randoms.map(r => insert.on("x" -> r)))

    batch.batch()

    def updateRow(row: UpdatableRow): Unit = {
      row("x") = row[Option[Int]]("x").map(_ + 1)
      row.updateRow()
    }

    val summary = selectForUpdate"SELECT * FROM tbl".copy(rowUpdater = updateRow).update()

    assertResult(UpdatableRow.Summary(updatedRows = batchCount))(summary)

    val incrementedFromDb = Select.vector[Int]("SELECT x FROM tbl ORDER BY x ASC")

    assert(incrementedFromDb.zip(incrementedRandoms).forall(xs => xs._1 == xs._2))
  }

  override protected def afterEach(): Unit = {
    withPg { implicit connection =>
      Ignore("DROP TABLE IF EXISTS tbl;").ignore()
    }
  }

}
