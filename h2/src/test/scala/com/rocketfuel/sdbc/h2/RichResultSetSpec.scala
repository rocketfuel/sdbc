package com.rocketfuel.sdbc.h2

import org.scalatest.BeforeAndAfterEach
import com.rocketfuel.sdbc.H2._
import scala.collection.immutable.Seq

class RichResultSetSpec
  extends H2Suite
  with BeforeAndAfterEach {

  test("iterator() works on a single result") {implicit connection =>
    val results = Select[Int]("SELECT 1").iterator().toSeq
    assertResult(Seq(1))(results)
  }

  test("iterator() works on several results") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt())
    Ignore.ignore("CREATE TABLE tbl (id identity PRIMARY KEY, x int)")

    val insert = Insert("INSERT INTO tbl (x) VALUES (@x)")
    val inserts = randoms.map(x => insert.on("x" -> x))

    val insertions = Batch.batch(Batch.toBatches(inserts))

    assertResult(randoms.size)(insertions.sum)

    val results = Select[Int]("SELECT x FROM tbl ORDER BY id ASC").iterator().toSeq

    assertResult(randoms)(results)
  }

  test("using SelectForUpdate to update a value works") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt()).sorted

    Ignore.ignore("CREATE TABLE tbl (id identity PRIMARY KEY, x int)")

    val insert = Batch.Part("INSERT INTO tbl (x) VALUES (@x)")
    val inserts = randoms.map(x => insert.on("x" -> x))

    Batch.batch(Batch.toBatches(inserts))

    SelectForUpdate("SELECT * FROM tbl", rowUpdater = {row => row("x") = row[Int]("x") + 1; row.updateRow()}).update()

    val afterUpdate = Select[Int]("SELECT x FROM tbl ORDER BY x ASC").iterator().toVector

    for ((afterUpdate, original) <- afterUpdate.zip(randoms)) {
      assertResult(original + 1)(afterUpdate)
    }
  }

  test("vector works") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt()).sorted

    Ignore.ignore("CREATE TABLE tbl (id identity PRIMARY KEY, x int)")

    val insert = Batch.Part("INSERT INTO tbl (x) VALUES (@x)")
    val inserts = randoms.map(x => insert.on("x" -> x))

    Batch.batch(Batch.toBatches(inserts))

    val result = Select.vector[Int]("SELECT x FROM tbl ORDER BY id ASC")

    assertResult(randoms)(result)
  }

  test("update works") {implicit connection =>
    Ignore.ignore("CREATE TABLE tbl (id identity PRIMARY KEY, x int)")

    val updateCount = Update.update("INSERT INTO tbl (id, x) VALUES (1, 1)")

    assertResult(1)(updateCount)
  }

  override protected def afterEach(): Unit = {
    Connection.using("jdbc:h2:mem:") { implicit connection =>
      Ignore.ignore("DROP TABLE IF EXISTS tbl")
    }
  }

}
