package com.rocketfuel.sdbc.base.jdbc

import org.scalatest._

class DefaultSettersSpec
  extends FunSuite {

  test("implicit Int conversion works") {
    assertCompiles("3: TestDbms.ParameterValue")
  }

  test("implicit Option[String] conversion works") {
    assertCompiles("Some(\"hello\"): TestDbms.ParameterValue")
  }

  test("implicit scala.BigDecimal conversion works") {
    assertCompiles("BigDecimal(1): TestDbms.ParameterValue")
  }

  test("implicit java.math.BigDecimal conversion works") {
    assertCompiles("BigDecimal(1).underlying: TestDbms.ParameterValue")
  }

}
