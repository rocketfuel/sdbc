package com.rocketfuel.sdbc.postgresql.implementation

import com.rocketfuel.sdbc.base.jdbc.statement.ParameterValue
import java.sql.{PreparedStatement, Time}
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import org.postgresql.util.PGobject

/**
  * This gives us better precision than the JDBC time type.
 *
  * @param localTime
  */
private[sdbc] class PGLocalTime(
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

private[sdbc] object PGLocalTime {
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

private[sdbc] trait LocalTimeParameter {
  self: ParameterValue =>

  implicit object LocalTimeParameter extends Parameter[LocalTime] {
    override val set: LocalTime => (PreparedStatement, Int) => PreparedStatement = {
      time => (statement, ix) =>
        val pgTime = PGLocalTime(time)
        statement.setObject(ix + 1, pgTime)
        statement
    }
  }

  implicit object TimeParameter extends Parameter[Time] {
    override val set: Time => (PreparedStatement, Int) => PreparedStatement = {
      time => (statement, ix) =>
        statement.setTime(ix + 1, time)
        statement
    }
  }

}
