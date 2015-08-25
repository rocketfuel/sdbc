package com.rocketfuel.sdbc.postgresql.jdbc

import org.scalatest._
import java.math.{BigDecimal => JBigDecimal}

class SettersSpec
  extends FunSuite {

  test("implicit Seq[Int] conversion works") {
    assertCompiles("val _: Option[ParameterValue[_]] = Seq(1,2,3)")
  }

  test("implicit Seq[String] conversion works") {
    assertCompiles("val _: Option[ParameterValue[_]] = Seq(\"\")")
  }

  test("implicit Seq[java.math.BigDecimal] conversion works") {
    assertCompiles("val _: Option[ParameterValue[_]] = Seq(1L,2L,3L).map(JBigDecimal.valueOf)")
  }

  test("implicit Seq[scala.BigDecimal] conversion works") {
    assertCompiles("val _: Option[ParameterValue[_]] = Seq(BigDecimal(1),BigDecimal(2),BigDecimal(3))")
  }

  test("implicit Seq[Option[Int]] conversion works") {
    assertCompiles("val _: Option[ParameterValue[_]] = Seq(Some(1),None,Some(3))")
  }

  test("implicit Seq[Option[java.math.BigDecimal]] conversion works") {
    assertCompiles("val _: Option[ParameterValue[_]] = Seq(Some(JBigDecimal.valueOf(1L)),None)")
  }

  test("implicit Seq[Option[scala.BigDecimal]] conversion works") {
    assertCompiles("val _: Option[ParameterValue[_]] = Seq(Some(BigDecimal(1)),None,Some(BigDecimal(3)))")
  }

}
