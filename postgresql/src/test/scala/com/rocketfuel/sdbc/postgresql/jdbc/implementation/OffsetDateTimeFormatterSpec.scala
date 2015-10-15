package com.rocketfuel.sdbc.postgresql.jdbc.implementation

import java.time.{OffsetDateTime, ZoneOffset}

import org.scalatest.FunSuite

class OffsetDateTimeFormatterSpec extends FunSuite {

  test("Can parse UTC timestamps from PostgreSql.") {
    val asString = "2014-10-21 20:40:56.586045+00"
    val manual = OffsetDateTime.of(2014, 10, 21, 20, 40, 56, 586045000, ZoneOffset.UTC)
    val parsed = OffsetDateTime.from(offsetDateTimeFormatter.parse(asString))
    assert(manual == parsed)
  }

  test("Can parse EST timestamps from PostgreSql.") {
    val asString = "2014-10-21 16:39:38.549548-04"
    val manual = OffsetDateTime.of(2014, 10, 21, 16, 39, 38, 549548000, ZoneOffset.ofHours(-4))
    val parsed = OffsetDateTime.from(offsetDateTimeFormatter.parse(asString))
    assert(manual == parsed)
  }

  test("Can parse timestamp with minute offsets from PostgreSql.") {
    val asString = "2014-10-22 02:06:25.003825+05:30"
    val manual = OffsetDateTime.of(2014, 10, 22, 2, 6, 25, 3825000, ZoneOffset.ofHoursMinutes(5, 30))
    val parsed = OffsetDateTime.from(offsetDateTimeFormatter.parse(asString))
    assert(manual == parsed)
  }

  test("Can parse timestamp with no fractional seconds from PostgreSql.") {
    val asString = "2014-10-21 15:59:08-04:30"
    val manual = OffsetDateTime.of(2014, 10, 21, 15, 59, 8, 0, ZoneOffset.ofHoursMinutes(-4, -30))
    val parsed =  OffsetDateTime.from(offsetDateTimeFormatter.parse(asString))
    assertResult(manual)(parsed)
  }

}
