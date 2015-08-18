package com.wda.sdbc.sqlserver.jdbc

import java.util.UUID

class UUIDSpec
  extends SqlServerSuite {

  test("UUID survives a round trip") { implicit connection =>
    val uuid = Some(UUID.randomUUID())
    val selected =
      Select[UUID]("SELECT CAST($uuid AS uniqueidentifier)").on(
        "uuid" -> uuid
      ).option()

    assertResult(uuid)(selected)
  }

  test("UUID survives a round trip as a string") { implicit connection =>
    val uuid = Some(UUID.randomUUID())
    val selected =
      Select[UUID]("SELECT $uuid").on(
        "uuid" -> uuid
      ).option()

    assertResult(uuid)(selected)
  }

}
