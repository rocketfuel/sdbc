package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.PostgreSql._
import java.math.{BigDecimal => JBigDecimal}
import org.scalatest._
import scala.xml.Elem

class ParameterValuesSpec
  extends FunSuite {

  val elem: Elem = <a></a>

  test("implicit None conversion works") {
    assertCompiles("None: ParameterValue")
  }

  test("implicit Elem conversion works") {
    assertCompiles("elem: ParameterValue")
  }

  test("implicit Seq[Int] conversion works") {
    assertCompiles("Seq[Int](): ParameterValue")
  }

  test("implicit Seq[String] conversion works") {
    assertCompiles("Seq[String](): ParameterValue")
  }

  test("implicit Seq[java.math.BigDecimal] conversion works") {
    assertCompiles("Seq[java.math.BigDecimal](): ParameterValue")
  }

  test("implicit Seq[scala.BigDecimal] conversion works") {
    assertCompiles("Seq[scala.BigDecimal](): ParameterValue")
  }

  test("implicit Seq[Option[Int]] conversion works") {
    assertCompiles("Seq[Option[Int]](): ParameterValue")
  }

  test("implicit Seq[Option[java.math.BigDecimal]] conversion works") {
    assertCompiles("Seq[Option[JBigDecimal]](): ParameterValue")
  }

  test("implicit Seq[Option[scala.BigDecimal]] conversion works") {
    assertCompiles("Seq[Option[BigDecimal]](): ParameterValue")
  }

  test("implicit Seq[java.lang.Long] conversion works") {
    assertCompiles("Seq[java.lang.Long](): ParameterValue")
  }

}
