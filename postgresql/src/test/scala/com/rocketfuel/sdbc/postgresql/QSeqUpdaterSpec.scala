package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.PostgreSql._

class QSeqUpdaterSpec
  extends PostgreSqlSuite {

  test("Updating an int[] works") {implicit connection =>
    Query[Unit]("CREATE TABLE tbl (id serial PRIMARY KEY, ints int[])").run()

    Query[Unit]("INSERT INTO tbl (ints) VALUES (@ints)").on("ints" -> QSeqUpdaterSpec.original).run()

    for (row <- QueryForUpdate("SELECT id, ints FROM tbl").run()) {
      row("ints") = QSeqUpdaterSpec.updated
      row.updateRow()
    }

    val selected = Query[Seq[Seq[Option[Int]]]]("SELECT ints FROM tbl").run()

    assertResult(Seq(QSeqUpdaterSpec.updated))(selected)

  }

}

object QSeqUpdaterSpec {
  val original = Seq(1,2,3)

  val updated = Seq(None, Some(2), Some(3))
}
