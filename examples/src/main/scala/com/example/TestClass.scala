package com.example

import com.rocketfuel.sdbc.H2._

case class TestClass(
  id: Int,
  value: String
)

object TestClass {

  final case class Value(value: String)

  object Value {
    implicit val selectable: Selectable[Value, TestClass] =
      Select[TestClass]("SELECT * FROM test_class WHERE value = @value").selectable[Value].product

    implicit val updatable: Updatable[Value] =
      Update("INSERT INTO test_class (value) VALUES (@value)").updatable[Value].product
  }

  final case class Id(id: Int)

  object Id {
    implicit val selectable: Selectable[Id, TestClass] =
      Select[TestClass]("SELECT * FROM test_class WHERE id = @id").selectable[Id].product
  }

  final case class All(newValue: String)

  object All {
    implicit val selectable: Selectable[All.type, TestClass] =
      Select[TestClass]("SELECT * FROM test_class").selectable[All.type].constant

    implicit val updatable: SelectForUpdatable[All] =
      SelectForUpdate("SELECT id, value FROM test_class").
      selectForUpdatable.constant {
        (key: All) => (row: UpdatableRow) =>
          row("value") = key.newValue
          row.updateRow()
      }
  }

}
