package com.example

import com.rocketfuel.sdbc.H2._

case class TestClass(
  id: Int,
  value: String
)

object TestClass {

  final case class Value(value: String)

  final case class Id(id: Int)

  case class All(newValue: String)

  implicit val selectableByValue = new Selectable[Value, TestClass] {
    val query = Select[TestClass]("SELECT * FROM test_class WHERE value = @value")

    override def select(key: Value): Select[TestClass] = {
      query.onProduct(key)
    }
  }

  implicit val selectableById: Selectable[Id, TestClass] =
    Select[TestClass]("SELECT * FROM test_class WHERE id = @id").selectable[Id].product

  implicit val selectableAll: Selectable[All.type, TestClass] =
    Select[TestClass]("SELECT * FROM test_class").selectable[All.type].constant

  implicit val insertValue: Updatable[Value] =
    Update("INSERT INTO test_class (value) VALUES (@value)").updatable[Value].product

  implicit val updateValues: SelectForUpdatable[All] =
    SelectForUpdate("SELECT id, value FROM test_class").
      selectForUpdatable.constant {
        (key: All) => (row: UpdatableRow) =>
          row("value") = key.newValue
          row.updateRow()
    }

}
