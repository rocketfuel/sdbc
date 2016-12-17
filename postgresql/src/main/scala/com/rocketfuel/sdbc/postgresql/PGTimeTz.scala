package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.base.jdbc.statement.ParameterValue
import java.time.OffsetTime
import org.postgresql.util.PGobject

private class PGTimeTz(
  var offsetTime: Option[OffsetTime]
) extends PGobject() {

  def this() {
    this(None)
  }

  setType("timetz")

  override def getValue: String = {
    offsetTime.map(offsetTimeFormatter.format).orNull
  }

  override def setValue(value: String): Unit = {
    this.offsetTime = for {
      reallyValue <- Option(value)
    } yield {
      val parsed = offsetTimeFormatter.parse(reallyValue)
      OffsetTime.from(parsed)
    }
  }

}

private object PGTimeTz {
  def apply(value: String): PGTimeTz = {
    val tz = new PGTimeTz()
    tz.setValue(value)
    tz
  }

  implicit def apply(value: OffsetTime): PGTimeTz = {
    new PGTimeTz(offsetTime = Some(value))
  }
}

trait OffsetTimeParameter {
  self: ParameterValue =>

  implicit val OffsetTimeParameter: Parameter[OffsetTime] =
    (offsetTime: OffsetTime) => {
      val timeTz = PGTimeTz(offsetTime)
      (statement: PreparedStatement, ix: Int) => {
        statement.setObject(ix + 1, timeTz)
        statement
      }
    }

}
