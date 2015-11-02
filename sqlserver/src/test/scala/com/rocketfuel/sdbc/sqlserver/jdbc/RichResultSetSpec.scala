package com.rocketfuel.sdbc.sqlserver.jdbc

import org.scalatest.BeforeAndAfterEach
import SqlServer._
import scala.collection.immutable.Seq

class RichResultSetSpec
  extends SqlServerSuite
  with BeforeAndAfterEach {

  test("seq() works on a single result") {implicit connection =>
    val results = Select[Int]("SELECT CAST(1 AS int)").iterator().toSeq
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

    assert(insertions.sum == randoms.size)

    val results = Select[Int]("SELECT x FROM tbl").iterator().toSeq
    assert(results == randoms)
  }

  test("using SelectForUpdate to update a value works") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt()).sorted

    Execute("CREATE TABLE tbl (id int IDENTITY(1,1) PRIMARY KEY, x int)").execute()

    val batch = randoms.foldLeft(Batch("INSERT INTO tbl (x) VALUES (@x)")) {
      case (batch, r) =>
        batch.addBatch("x" -> r)
    }

    batch.iterator()

    for (row <- connection.iteratorForUpdate("SELECT x FROM tbl")) {
      row("x") = row.get[Int]("x").map(_ + 1)
      row.updateRow()
    }

    val afterUpdate = connection.iterator[Int]("SELECT x FROM tbl ORDER BY x ASC").toSeq

    for ((afterUpdate, original) <- afterUpdate.zip(randoms)) {
      assertResult(original + 1)(afterUpdate)
    }
  }

  override protected def afterEach(): Unit = {
    withSql(_.execute("IF object_id('dbo.tbl') IS NOT NULL DROP TABLE tbl"))
  }
}
