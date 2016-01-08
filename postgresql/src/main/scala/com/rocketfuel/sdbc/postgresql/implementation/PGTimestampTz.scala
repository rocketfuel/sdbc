package com.rocketfuel.sdbc.postgresql.implementation

import com.rocketfuel.sdbc.base.jdbc.ParameterValue
import java.time.OffsetDateTime
import org.postgresql.util.PGobject

private[sdbc] class PGTimestampTz(
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

private[sdbc] object PGTimestampTz {
  def apply(value: String): PGTimestampTz = {
    val tz = new PGTimestampTz()
    tz.setValue(value)
    tz
  }

  implicit def apply(value: OffsetDateTime): PGTimestampTz = {
    new PGTimestampTz(offsetDateTime = Some(value))
  }
}

private[sdbc] trait OffsetDateTimeParameter {
  self: ParameterValue =>

  implicit object OffsetDateTimeParameter extends Parameter[OffsetDateTime] {
    override val set: OffsetDateTime => (PreparedStatement, Int) => PreparedStatement = {
      dt => (statement, ix) =>
        val pgDt = PGTimestampTz(dt)
        statement.setObject(ix + 1, pgDt)
        statement
    }
  }

}
