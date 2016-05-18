package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.PostgreSql._

class QSeqUpdaterSpec
  extends PostgreSqlSuite {

  test("Updating an int[] works") {implicit connection =>
    Select[UpdateCount]("CREATE TABLE tbl (id serial PRIMARY KEY, ints int[])").run()

    Select[UpdateCount]("INSERT INTO tbl (ints) VALUES (@ints)").on("ints" -> QSeqUpdaterSpec.original).run()

    Select[CloseableIterator[UpdatableRow]]("SELECT id, ints FROM tbl").run().foreach {
      row =>
        row("ints") = QSeqUpdaterSpec.updated
        row.updateRow()
    }

    val selected = Select[Vector[Option[Int]]]("SELECT ints FROM tbl").run().toSeq

    assertResult(Vector(QSeqUpdaterSpec.updated))(selected)

  }

}

object QSeqUpdaterSpec {
  val original = Seq(1,2,3)

  val updated = Seq(None, Some(2), Some(3))
}
