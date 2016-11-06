package com.rocketfuel.sdbc.base.jdbc

import org.scalatest.FunSuite
import TestDbms._
import shapeless._

class MultiResultConverterSpec
  extends FunSuite {

  test("Unit") {
    assertCompiles("MultiQuery[Unit](\"\")")
  }

  test("(Unit, Unit)") {
    assertCompiles("MultiQuery[(Unit, Unit)](\"\")")
  }

  test("HNil") {
    assertCompiles("MultiQuery[HNil](\"\")")
  }

  test("Unit::HNil") {
    assertCompiles("MultiQuery[Unit::HNil](\"\")")
  }

  test("Unit::Unit::HNil") {
    assertCompiles("MultiQuery[Unit::Unit::HNil](\"\")")
  }

  test("QueryResult.UpdateCount") {
    assertCompiles("MultiQuery[QueryResult.UpdateCount](\"\")")
  }

  test("(Unit, QueryResult.UpdateCount)") {
    assertCompiles("MultiQuery[(Unit, QueryResult.UpdateCount)](\"\")")
  }

}
