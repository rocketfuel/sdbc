package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.base.jdbc.statement.ParameterValue
import java.time.OffsetDateTime
import org.postgresql.util.PGobject

private class PGTimestampTz(
  var offsetDateTime: Option[OffsetDateTime]
) extends PGobject() {

  def this() {
    this(None)
  }

  setType("timestamptz")

  override def getValue: String = {
    offsetDateTime.map(offsetDateTimeFormatter.format).orNull
  }

  override def setValue(value: String): Unit = {
    offsetDateTime = for {
      reallyValue <- Option(value)
    } yield {
        val parsed = offsetDateTimeFormatter.parse(reallyValue)
        OffsetDateTime.from(parsed)
      }
  }

}

private object PGTimestampTz {
  def apply(value: String): PGTimestampTz = {
    val tz = new PGTimestampTz()
    tz.setValue(value)
    tz
  }

  implicit def apply(value: OffsetDateTime): PGTimestampTz = {
    new PGTimestampTz(offsetDateTime = Some(value))
  }
}

trait OffsetDateTimeParameter {
  self: ParameterValue =>

  implicit val OffsetDateTimeParameter: Parameter[OffsetDateTime] =
    (dt: OffsetDateTime) => {
      val pgDt = PGTimestampTz(dt)
      (statement: PreparedStatement, ix: Int) => {
        statement.setObject(ix + 1, pgDt)
        statement
      }
    }

}
