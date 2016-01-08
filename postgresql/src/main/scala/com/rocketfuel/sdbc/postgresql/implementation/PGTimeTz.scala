package com.rocketfuel.sdbc.postgresql.implementation

import com.rocketfuel.sdbc.base.jdbc.ParameterValue
import java.time.OffsetTime
import org.postgresql.util.PGobject

private[sdbc] class PGTimeTz(
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

private[sdbc] object PGTimeTz {
  def apply(value: String): PGTimeTz = {
    val tz = new PGTimeTz()
    tz.setValue(value)
    tz
  }

  implicit def apply(value: OffsetTime): PGTimeTz = {
    new PGTimeTz(offsetTime = Some(value))
  }
}

private[sdbc] trait OffsetTimeParameter {
  self: ParameterValue =>

  implicit object OffsetTimeParameter extends Parameter[OffsetTime] {
    override val set: (OffsetTime) => (PreparedStatement, Int) => PreparedStatement = {
      offsetTime => (statement, ix) =>
        val timeTz = PGTimeTz(offsetTime)
        statement.setObject(ix + 1, timeTz)
        statement
    }
  }

}
