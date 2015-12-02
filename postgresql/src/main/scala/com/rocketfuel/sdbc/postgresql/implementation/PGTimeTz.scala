package com.rocketfuel.sdbc.postgresql.implementation

import java.time.OffsetTime

import com.rocketfuel.sdbc.base.ToParameter
import org.postgresql.util.PGobject

private[sdbc] class PGTimeTz() extends PGobject() {

  setType("timetz")

  var offsetTime: Option[OffsetTime] = None

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

private[sdbc] object PGTimeTz extends ToParameter {
  def apply(value: String): PGTimeTz = {
    val tz = new PGTimeTz()
    tz.setValue(value)
    tz
  }

  def apply(value: OffsetTime): PGTimeTz = {
    val tz = new PGTimeTz()
    tz.offsetTime = Some(value)
    tz
  }

  val toParameter: PartialFunction[Any, Any] = {
    case o: OffsetTime => PGTimeTz(o)
  }
}

private[sdbc] trait PGTimeTzImplicits {
  implicit def OffsetTimeToPGobject(o: OffsetTime): PGobject = {
    PGTimeTz(o)
  }
}
