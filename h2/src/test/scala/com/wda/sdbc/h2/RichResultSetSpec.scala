package com.wda.sdbc.h2

import com.wda.sdbc.H2._
import org.scalatest.BeforeAndAfterEach

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
    Update("CREATE TABLE tbl (x int)").execute()

    val batch = randoms.foldLeft(Batch("INSERT INTO tbl (x) VALUES ($x)")) {
      case (batch, r) =>
        batch.addBatch("x" -> r)
    }

    val insertions = batch.iterator

    assertResult(randoms.size)(insertions.sum)

    val results = Select[Int]("SELECT x FROM tbl").iterator().toSeq

    assertResult(randoms)(results)
  }

  test("using SelectForUpdate to update a value works") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt()).sorted

    Update("CREATE TABLE tbl (id identity PRIMARY KEY, x int)").execute()

    val batch = randoms.foldLeft(Batch("INSERT INTO tbl (x) VALUES ($x)")) {
      case (batch, r) =>
        batch.addBatch("x" -> r)
    }

    batch.execute()

    for(row <- connection.iteratorForUpdate("SELECT * FROM tbl")) {
      row("x") = row[Int]("x").map(_ + 1)
      row.updateRow()
    }

    val afterUpdate = connection.iterator[Int]("SELECT x FROM tbl ORDER BY x ASC").toSeq

    for ((afterUpdate, original) <- afterUpdate.zip(randoms)) {
      assertResult(original + 1)(afterUpdate)
    }
  }

  override protected def afterEach(): Unit = {
    withMemConnection()(_.execute("DROP TABLE IF EXISTS tbl"))
  }

}
