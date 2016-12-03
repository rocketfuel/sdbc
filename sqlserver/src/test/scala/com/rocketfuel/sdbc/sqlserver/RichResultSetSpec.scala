package com.rocketfuel.sdbc.sqlserver

import org.scalatest.BeforeAndAfterEach
import scala.collection.immutable.Seq
import com.rocketfuel.sdbc.SqlServer._

class RichResultSetSpec
  extends SqlServerSuite
  with BeforeAndAfterEach {

  test("iterator() works on a single result") {implicit connection =>
    val results = Select[Int]("SELECT CAST(1 AS int)").iterator().toVector
    assertResult(Vector(1))(results)
  }

  test("iterator() works on several results") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt())
    Ignore.ignore("CREATE TABLE tbl (x int)")

    val batch = randoms.foldLeft(Batch("INSERT INTO tbl (x) VALUES (@x)")) {
      case (batch, r) =>
        batch.add("x" -> r)
    }

    val insertions = batch.batch()

    assertResult(randoms.size)(insertions.sum)

    val results = Select[Int]("SELECT x FROM tbl").iterator.toVector
    assertResult(randoms)(results)
  }

  test("using Query[CloseableIterator[UpdatableRow]] to update a value works") {implicit connection =>
    val randoms = Seq.fill(10)(util.Random.nextInt()).sorted

    Ignore.ignore("CREATE TABLE tbl (id int IDENTITY(1,1) PRIMARY KEY, x int)")

    val batch = randoms.foldLeft(Batch("INSERT INTO tbl (x) VALUES (@x)")) {
      case (batch, r) =>
        batch.add("x" -> r)
    }

    batch.batch()

    val select =
      Select[Int]("SELECT x FROM tbl ORDER BY id ASC")

    val beforeUpdate = select.iterator().toVector

    for (row <- SelectForUpdate("SELECT x FROM tbl").update()) {
      row("x") = row[Option[Int]]("x").map(_ + 1)
      row.updateRow()
    }

    val afterUpdate = select.iterator().toVector

    for ((afterUpdate, original) <- afterUpdate.zip(randoms)) {
      assertResult(original + 1)(afterUpdate)
    }
  }

  override protected def afterEach(): Unit = {
    withSql { implicit connection =>
      Ignore.ignore("IF object_id('dbo.tbl') IS NOT NULL DROP TABLE tbl")
    }
  }
}
