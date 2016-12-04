package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.base.jdbc.statement.{DateParameter, ParameterValue}
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import org.postgresql.util.PGobject

/**
  * This gives us better precision than the JDBC time type.
 *
  * @param localTime
  */
private class PGLocalTime(
  var localTime: Option[LocalTime]
) extends PGobject() {

  def this() {
    this(None)
  }

  setType("time")

  override def getValue: String = {
    localTime.map(DateTimeFormatter.ISO_LOCAL_TIME.format).orNull
  }

  override def setValue(value: String): Unit = {
    this.localTime = for {
      reallyValue <- Option(value)
    } yield {
      PGLocalTime.parse(reallyValue)
    }
  }
}

private object PGLocalTime {
  def apply(value: String): PGLocalTime = {
    val t = new PGLocalTime()
    t.setValue(value)
    t
  }

  implicit def apply(l: LocalTime): PGLocalTime = {
    new PGLocalTime(Some(l))
  }

  def parse(value: String): LocalTime = {
    val parsed = DateTimeFormatter.ISO_LOCAL_TIME.parse(value)
    LocalTime.from(parsed)
  }
}
