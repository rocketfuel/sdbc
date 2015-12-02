package com.rocketfuel.sdbc.postgresql.implementation

import java.time.OffsetDateTime

import com.rocketfuel.sdbc.base.ToParameter
import org.postgresql.util.PGobject

private[sdbc] class PGTimestampTz() extends PGobject() {

  setType("timestamptz")

  var offsetDateTime: Option[OffsetDateTime] = None

  override def getValue: String = {
    offsetDateTime.map(offsetDateTimeFormatter.format).orNull
  }

  override def setValue(value: String): Unit = {
    this.offsetDateTime = for {
      reallyValue <- Option(value)
    } yield {
        val parsed = offsetDateTimeFormatter.parse(reallyValue)
        OffsetDateTime.from(parsed)
      }
  }

}

private[sdbc] object PGTimestampTz extends ToParameter {
  def apply(value: String): PGTimestampTz = {
    val tz = new PGTimestampTz()
    tz.setValue(value)
    tz
  }

  def apply(value: OffsetDateTime): PGTimestampTz = {
    val tz = new PGTimestampTz()
    tz.offsetDateTime = Some(value)
    tz
  }

  val toParameter: PartialFunction[Any, Any] = {
    case o: OffsetDateTime => PGTimestampTz(o)
  }
}

private[sdbc] trait PGTimestampTzImplicits {
  implicit def OffsetDateTimeToPGobject(o: OffsetDateTime): PGobject = {
    PGTimestampTz(o)
  }
}
