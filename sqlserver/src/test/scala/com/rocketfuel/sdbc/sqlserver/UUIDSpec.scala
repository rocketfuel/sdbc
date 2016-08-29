package com.rocketfuel.sdbc.sqlserver

import java.util.UUID
import com.rocketfuel.sdbc.SqlServer._

class UUIDSpec
  extends SqlServerSuite {

  test("UUID survives a round trip") { implicit connection =>
    val uuid = Some(UUID.randomUUID())
    val selected =
      Select[Option[UUID]]("SELECT CAST(@uuid AS uniqueidentifier)").on(
        "uuid" -> uuid
      ).option().flatten

    assertResult(uuid)(selected)
  }

  test("UUID survives a round trip as a string") { implicit connection =>
    val uuid = Some(UUID.randomUUID())
    val selected =
      Select[Option[UUID]]("SELECT @uuid").on(
        "uuid" -> uuid
      ).option().flatten

    assertResult(uuid)(selected)
  }

}
