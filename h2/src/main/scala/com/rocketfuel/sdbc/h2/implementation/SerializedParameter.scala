package com.rocketfuel.sdbc.h2.implementation

import java.sql.{PreparedStatement, Types}
import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.h2.Serialized

private[sdbc] trait SerializedParameter {

  implicit def SerializedParameter = new IsParameter[Serialized] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Serialized): Unit = {
      preparedStatement.setObject(parameterIndex, parameter.value, Types.JAVA_OBJECT)
    }
  }

  implicit def SerializedToParameterValue(s: Serialized): ParameterValue = {
    ParameterValue(s)
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
