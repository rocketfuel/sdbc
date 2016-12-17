package com.rocketfuel.sdbc.h2

import java.sql.Types

trait SerializedParameter {
  self: H2 =>

  implicit val SerializedParameter: Parameter[Serialized] = {
      (value: Serialized) => (statement: PreparedStatement, index: Int) =>
        statement.setObject(index + 1, value.value, Types.JAVA_OBJECT)
        statement
    }

  implicit val SerializedUpdater: Updater[Serialized] =
    (row: UpdatableRow, columnIndex: Int, x: Serialized) =>
      row.updateObject(columnIndex + 1, x.value, Types.JAVA_OBJECT)

  implicit val SerializedGetter: Getter[Serialized] = {
    (row: Row, ix: Int) =>
      Option(row.getObject(ix)).map(o => Serialized(o.asInstanceOf[AnyRef with java.io.Serializable]))
  }

}
