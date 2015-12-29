package com.rocketfuel.sdbc.h2.implementation

import com.rocketfuel.sdbc.base.{ParameterValue, box}

private[sdbc] case class QSeq(
  value: Seq[ParameterValue]
) {
  def asJavaArray: Array[AnyRef] = {
    QSeq.toJavaArray(this)
  }
}

private[sdbc] object QSeq extends ToParameter {

  override val toParameter: PartialFunction[Any, Any] = {
    case s: QSeq => s
  }

  private def toJavaArray(q: QSeq): Array[AnyRef] = {
    q.value.map(_.map {
      case ParameterValue(innerSeq: QSeq) =>
        toJavaArray(innerSeq)
      case ParameterValue(otherwise) =>
        box(otherwise)
    }.orNull).toArray
  }

}
