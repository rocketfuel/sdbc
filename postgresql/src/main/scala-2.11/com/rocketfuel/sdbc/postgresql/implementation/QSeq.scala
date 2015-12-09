package com.rocketfuel.sdbc.postgresql.implementation

import java.sql.Connection
import com.rocketfuel.sdbc.base._

private[sdbc] case class QSeq[T](
  value: Seq[ParameterValue],
  typeName:  String
) {
  def asJavaArray: Array[AnyRef] = {
    QSeq.toJavaArray(this)
  }

  def asJdbcArray(connection: Connection): java.sql.Array = {
    connection.createArrayOf(typeName, asJavaArray)
  }
}

private[sdbc] object QSeq extends ToParameter {

  override val toParameter: PartialFunction[Any, Any] = {
    case s: QSeq[_] => s
  }

  private def toJavaArray(q: QSeq[_]): Array[AnyRef] = {
    q.value.map(_.map {
      case ParameterValue(innerSeq: QSeq[_]) =>
        toJavaArray(innerSeq)
      case ParameterValue(otherwise) =>
        box(otherwise)
    }.orNull).toArray
  }

}
