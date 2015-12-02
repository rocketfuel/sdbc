package com.rocketfuel.sdbc.postgresql.implementation

import java.time.LocalTime
import java.time.format.DateTimeFormatter

import com.rocketfuel.sdbc.base.ToParameter
import org.postgresql.util.PGobject

private[sdbc] class PGLocalTime() extends PGobject() {

  setType("time")

  var localTime: Option[LocalTime] = None

  override def getValue: String = {
    localTime.map(DateTimeFormatter.ISO_LOCAL_TIME.format).orNull
  }

  override def setValue(value: String): Unit = {
    this.localTime = for {
      reallyValue <- Option(value)
    } yield {
        val parsed = DateTimeFormatter.ISO_LOCAL_TIME.parse(reallyValue)
        LocalTime.from(parsed)
      }
  }
}

private[sdbc] object PGLocalTime extends ToParameter {
  def apply(l: LocalTime): PGLocalTime = {
    val t = new PGLocalTime()
    t.localTime = Some(l)
    t
  }

  override val toParameter: PartialFunction[Any, Any] = {
    case l: LocalTime => PGLocalTime(l)
  }
}

private[sdbc] trait PGLocalTimeImplicits {
  implicit def LocalTimeToPGobject(l: LocalTime): PGobject = {
    PGLocalTime(l)
  }
}
