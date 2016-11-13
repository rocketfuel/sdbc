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

    val batch = randoms.foldLeft(Batch("INSERT INTO tbl (x) VALUES (@x)")) {
      case (batch, r) =>
        batch.add("x" -> r)
    }

    val insertions = batch.batch()

    assertResult(randoms.size)(insertions.sum)

    val results = Select[Int]("SELECT x FROM tbl ORDER BY id ASC").iterator().toSeq

    assertResult(randoms)(results)
  }

  test("using SelectForUpdate to update a value works") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt()).sorted

    Ignore.ignore("CREATE TABLE tbl (id identity PRIMARY KEY, x int)")

    val batch = randoms.foldLeft(Batch("INSERT INTO tbl (x) VALUES (@x)")) {
      case (batch, r) =>
        batch.add("x" -> r)
    }

    batch.batch()

    for(row <- SelectForUpdate("SELECT * FROM tbl").iterator()) {
      row("x") = row[Int]("x") + 1
      row.updateRow()
    }

    val afterUpdate = Select[Int]("SELECT x FROM tbl ORDER BY x ASC").iterator().toVector

    for ((afterUpdate, original) <- afterUpdate.zip(randoms)) {
      assertResult(original + 1)(afterUpdate)
    }
  }

  test("to[Vector] works") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt()).sorted

    Ignore.ignore("CREATE TABLE tbl (id identity PRIMARY KEY, x int)")

    val batch = randoms.foldLeft(Batch("INSERT INTO tbl (x) VALUES (@x)")) {
      case (batch, r) =>
        batch.add("x" -> r)
    }

    batch.batch()

    val result = Select[Int]("SELECT x FROM tbl ORDER BY id ASC").iterator().toVector

    assertResult(randoms)(result)
  }

  override protected def afterEach(): Unit = {
    withMemConnection() { implicit connection =>
      Ignore.ignore("DROP TABLE IF EXISTS tbl")
    }
  }

}
