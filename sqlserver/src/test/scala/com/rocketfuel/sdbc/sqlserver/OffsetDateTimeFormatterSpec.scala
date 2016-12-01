package com.rocketfuel.sdbc.sqlserver

import com.rocketfuel.sdbc.SqlServer._
import java.time.{OffsetDateTime, ZoneOffset}
import org.scalatest.FunSuite

class OffsetDateTimeFormatterSpec extends FunSuite {

  test("Can parse UTC timestamps from SqlServer.") {
    val asString = "2014-10-21 19:55:15.9600000 +00:00"
    val manual = OffsetDateTime.of(2014, 10, 21, 19, 55, 15, 960000000, ZoneOffset.UTC)
    val parsed = OffsetDateTime.from(offsetDateTimeFormatter.parse(asString))
    assert(manual == parsed)
  }

  test("Can parse EST timestamps from SqlServer.") {
    val asString = "2014-10-21 15:59:08.2405747 -04:00"
    val manual = OffsetDateTime.of(2014, 10, 21, 15, 59, 8, 240574700, ZoneOffset.ofHours(-4))
    val parsed = OffsetDateTime.from(offsetDateTimeFormatter.parse(asString))
    assert(manual == parsed)
  }

  test("Can parse timestamp with minute offsets from SqlServer.") {
    val asString = "2014-10-21 15:59:08.2405747 -04:30"
    val manual = OffsetDateTime.of(2014, 10, 21, 15, 59, 8, 240574700, ZoneOffset.ofHoursMinutes(-4, -30))
    val parsed = OffsetDateTime.from(offsetDateTimeFormatter.parse(asString))
    assert(manual == parsed)
  }

  test("Can parse timestamp with no fractional seconds from SqlServer.") {
    val asString = "2014-10-21 15:59:08 -04:30"
    val manual = OffsetDateTime.of(2014, 10, 21, 15, 59, 8, 0, ZoneOffset.ofHoursMinutes(-4, -30))
    val parsed =  OffsetDateTime.from(offsetDateTimeFormatter.parse(asString))
    assertResult(manual)(parsed)
  }

}
