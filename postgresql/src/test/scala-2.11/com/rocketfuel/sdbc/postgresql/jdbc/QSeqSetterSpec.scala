package com.rocketfuel.sdbc.postgresql.jdbc

import com.rocketfuel.sdbc.postgresql._
import org.scalatest._

class QSeqSetterSpec extends FunSuite {

  test("implicit Seq[Int] conversion works") {
    assertCompiles("val _: Option[ParameterValue] = Seq(1,2,3)")
  }

  test("implicit Seq[Option[Int]] conversion works") {
    assertCompiles("val _: Option[ParameterValue] = Seq(1,2,3).map(Some.apply)")
  }

  test("implicit Seq[Seq[Int]] conversion works") {
    assertCompiles("val _: Option[ParameterValue] = Seq(Seq(1),Seq(2),Seq(3))")
  }

  test("implicit Seq[Option[Seq[Int]]] conversion works") {
    assertCompiles("val _: Option[ParameterValue] = Seq(Some(Seq(1)),None,Some(Seq(3)))")
  }

  test("implicit Seq[Seq[Option[Int]]] conversion works") {
    assertCompiles("val _: Option[ParameterValue] = Seq(Seq(Some(1), Some(2)), Seq(None))")
  }

}
