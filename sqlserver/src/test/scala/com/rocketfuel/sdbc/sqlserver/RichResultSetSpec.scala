package com.rocketfuel.sdbc.sqlserver

import org.scalatest.BeforeAndAfterEach
import scala.collection.immutable.Seq
import com.rocketfuel.sdbc.SqlServer._
import com.rocketfuel.sdbc.base.CloseableIterator

class RichResultSetSpec
  extends SqlServerSuite
  with BeforeAndAfterEach {

  test("Vector[Int] works on a single result") {implicit connection =>
    val results = Select[Vector[Int]]("SELECT CAST(1 AS int)").run()
    assert(results == Vector(1))
  }

  test("Vector[Int] works on several results") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt())
    Select[Unit]("CREATE TABLE tbl (x int)").run()

    val batch = randoms.foldLeft(Batch("INSERT INTO tbl (x) VALUES (@x)")) {
      case (batch, r) =>
        batch.addBatch("x" -> r)
    }

    val insertions = batch.run()

    assert(insertions.sum == randoms.size)

    val results = Select[Vector[Int]]("SELECT x FROM tbl").run()
    assert(results == randoms)
  }

  test("using Query[CloseableIterator[UpdatableRow]] to update a value works") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt()).sorted

    Select[Unit]("CREATE TABLE tbl (id int IDENTITY(1,1) PRIMARY KEY, x int)").run()

    val batch = randoms.foldLeft(Batch("INSERT INTO tbl (x) VALUES (@x)")) {
      case (batch, r) =>
        batch.addBatch("x" -> r)
    }

    batch.run()

    for (row <- Select[CloseableIterator[UpdatableRow]]("SELECT x FROM tbl").run()) {
      row("x") = row[Option[Int]]("x").map(_ + 1)
      row.updateRow()
    }

    val afterUpdate = Select[Vector[Int]]("SELECT x FROM tbl ORDER BY x ASC").run()

    for ((afterUpdate, original) <- afterUpdate.zip(randoms)) {
      assertResult(original + 1)(afterUpdate)
    }
  }

  override protected def afterEach(): Unit = {
    withSql { implicit connection =>
      Select[Unit]("IF object_id('dbo.tbl') IS NOT NULL DROP TABLE tbl").run()
    }
  }
}
