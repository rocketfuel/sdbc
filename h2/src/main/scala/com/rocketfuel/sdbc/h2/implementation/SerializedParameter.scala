package com.rocketfuel.sdbc.h2.implementation

import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.h2.Serialized
import java.sql.Types

private[sdbc] trait SerializedParameter {
  self: ParameterValue
    with Updater
    with UpdatableRow
    with ParameterValue
    with MutableRow
    with Row
    with Getter =>

  implicit object SerializedParameter
    extends PrimaryParameter[Serialized] {
    override val toParameter: PartialFunction[Any, Any] = {
      case s: Serialized => s
    }
    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case Serialized(value) =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setObject(ix, value, Types.JAVA_OBJECT)
          statement
    }
  }

  implicit val SerializedUpdater: Updater[Serialized] =
    new Updater[Serialized] {
      override def update(row: UpdatableRow, columnIndex: Int, x: Serialized): Unit = {
        row.updateObject(columnIndex, x.value, Types.JAVA_OBJECT)
      }
    }

  implicit val SerializedGetter: Getter[Serialized] = {
    (row: Row, ix: Index) =>
      Option(row.getObject(ix(row))).map(o => Serialized(o.asInstanceOf[AnyRef with java.io.Serializable]))
  }

}
