package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.PostgreSqlArgonaut._

class QSeqUpdaterSpec
  extends PostgreSqlSuite {

  test("Updating an int[] works") {implicit connection =>
    Update.update("CREATE TABLE tbl (id serial PRIMARY KEY, ints int[])")

    Update("INSERT INTO tbl (ints) VALUES (@ints)").on("ints" -> QSeqUpdaterSpec.original).update()

    def updater(row: UpdatableRow): Unit = {
      row("ints") = QSeqUpdaterSpec.updated
      row.updateRow()
    }

    SelectForUpdate.update("SELECT id, ints FROM tbl", rowUpdater = updater)

    val selected: Seq[Seq[Option[Int]]] =
      Select.vector[Seq[Option[Int]]]("SELECT ints FROM tbl")

    assertResult(Seq(QSeqUpdaterSpec.updated))(selected)
  }

}

object QSeqUpdaterSpec {
  val original = Seq(1,2,3)

  val updated = Seq(None, Some(2), Some(3))
}
