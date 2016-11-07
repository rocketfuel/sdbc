package com.rocketfuel.sdbc.base.jdbc.statement

import com.rocketfuel.sdbc.base.jdbc.TestDbms._
import org.scalatest.FunSuite
import shapeless._
import shapeless.syntax.std.tuple._

class MultiResultConverterSpec
  extends FunSuite {

  implicit def c: Connection = ???

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

  test("QueryResult.Update") {
    assertCompiles("MultiQuery[QueryResult.Update](\"\")")
  }

  test("(QueryResult.Unit, QueryResult.Update)") {
    assertCompiles("MultiQuery[(QueryResult.Unit, QueryResult.Update)](\"\")")
  }

  test("(QueryResult.Unit, QueryResult.Update).map(QueryResult.get)") {
    assertCompiles(
      "val ((), u: Long) = MultiQuery.run[(QueryResult.Unit, QueryResult.Update)](\"\").map(QueryResult.get)"
    )
  }

}
