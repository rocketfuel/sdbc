package com.rocketfuel.sdbc.examples

import com.rocketfuel.sdbc.h2.jdbc._

case class TestClass(
  id: Int,
  value: String
)

object TestClass {

  implicit def apply(row: Row): TestClass = {
    TestClass(row.get[Int]("id").get, row.get[String]("value").get)
  }

  implicit val selectableByValue = new Selectable[Value, TestClass] {
    val query = Select[TestClass]("SELECT * FROM test_class WHERE value = @value")

    override def select(key: Value): Select[TestClass] = {
      query.on("value" -> key.value)
    }
  }

  implicit val selectableById = new Selectable[Id, TestClass] {
    val query = Select[TestClass]("SELECT * FROM test_class WHERE id = @id")

    override def select(key: Id): Select[TestClass] = {
      query.on("id" -> key.id)
    }
  }

  implicit val selectableAll = new Selectable[All.type, TestClass] {
    val query = Select[TestClass]("SELECT * FROM test_class")

    override def select(key: All.type): Select[TestClass] = {
      query
    }
  }

  implicit val insertValue = new Updatable[Value] {
    val query = Update("INSERT INTO test_class (value) VALUES (@value)")

    override def update(key: Value): Update = {
      query.on("value" -> key.value)
    }
  }

  final case class Value(value: String)

  final case class Id(id: Int)

  case object All

}
