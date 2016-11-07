package com.rocketfuel.sdbc.base.jdbc

import org.scalatest.FunSuite
import TestDbms._
import shapeless._
import shapeless.ops.tuple.Mapper
import shapeless.syntax.std.tuple._

class MultiResultConverterSpec
  extends FunSuite {
  test("Unit") {
    assertCompiles("MultiQuery[QueryResult.Unit](\"\")")
  }

  test("(Unit, Unit)") {
    assertCompiles("MultiQuery[(QueryResult.Unit, Unit)](\"\")")
  }

  test("HNil") {
    assertCompiles("MultiQuery[HNil](\"\")")
  }

  test("Unit::HNil") {
    assertCompiles("MultiQuery[QueryResult.Unit::HNil](\"\")")
  }

  test("Unit::Unit::HNil") {
    assertCompiles("MultiQuery[QueryResult.Unit::QueryResult.Unit::HNil](\"\")")
  }

  test("QueryResult.UpdateCount") {
    assertCompiles("MultiQuery[QueryResult.UpdateCount](\"\")")
  }

  test("(Unit, QueryResult.UpdateCount)") {
    assertCompiles("MultiQuery[(QueryResult.Unit, QueryResult.UpdateCount)](\"\")")
  }

  test("(Unit, QueryResult.UpdateCount).map(QueryResult.get)") {
    assertCompiles(
      """implicit def c: TestDbms.Connection = ???
        |implicit def r = MultiQuery[(QueryResult.Unit, QueryResult.UpdateCount)]("").run().map(QueryResult.get): (Unit, Long)
        |""".stripMargin)
  }

}
