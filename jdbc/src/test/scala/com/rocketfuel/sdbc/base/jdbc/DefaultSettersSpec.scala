package com.rocketfuel.sdbc.base.jdbc

import org.scalatest._

class DefaultSettersSpec
  extends FunSuite {

  test("implicit Int conversion works") {
    assertCompiles("val _: TestDbms.ParameterValue = 3")
  }

  test("implicit Option[String] conversion works") {
    assertCompiles("val _: TestDbms.ParameterValue = Some(\"hello\")")
  }

  test("implicit scala.BigDecimal conversion works") {
    assertCompiles("val _: TestDbms.ParameterValue = BigDecimal(1)")
  }

  test("implicit java.math.BigDecimal conversion works") {
    assertCompiles("val _: TestDbms.ParameterValue = BigDecimal(1).underlying")
  }

  test("Row#get works") {
    assertCompiles("val row: TestDbms.Row = ???; val _ = row.get[String](???)")
  }

}
