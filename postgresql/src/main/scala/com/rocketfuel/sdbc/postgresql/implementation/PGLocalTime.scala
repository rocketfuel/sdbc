package com.rocketfuel.sdbc.postgresql.implementation

import com.rocketfuel.sdbc.base.jdbc.ParameterValue
import java.time.LocalTime
import java.time.format.DateTimeFormatter
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

private[sdbc] object PGLocalTime {
  implicit def apply(l: LocalTime): PGLocalTime = {
    val t = new PGLocalTime()
    t.localTime = Some(l)
    t
  }
}

private[sdbc] trait LocalTimeParameter {
  self: ParameterValue =>

  implicit object LocalTimeParameter extends Parameter[LocalTime] {
    override val set: (LocalTime) => (Statement, Int) => Statement = {
      time => (statement, ix) =>
        val pgTime: PGLocalTime = time
        statement.setObject(ix + 1, pgTime)
        statement
    }
  }

}
