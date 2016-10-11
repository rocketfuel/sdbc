package com.rocketfuel.sdbc.postgresql

import org.scalatest._
import com.rocketfuel.sdbc.PostgreSql._

class QSeqSetterSpec extends FunSuite {

  test("implicit Seq[Int] conversion works") {
    assertCompiles("Seq(1,2,3): ParameterValue")
  }

  test("implicit Seq[Option[Int]] conversion works") {
    Seq(1,2,3).map(Some.apply): ParameterValue
    assertCompiles("Seq(1,2,3).map(Some.apply): ParameterValue")
  }

  test("implicit Seq[Seq[Int]] conversion works") {
    assertCompiles("Seq(Seq(1),Seq(2),Seq(3)): ParameterValue")
  }

  test("implicit Seq[Option[Seq[Int]]] conversion works") {
    assertCompiles("Seq(Some(Seq(1)),None,Some(Seq(3))): ParameterValue")
  }

  test("implicit Seq[Seq[Option[Int]]] conversion works") {
    assertCompiles("Seq(Seq(Some(1), Some(2)), Seq(None)): ParameterValue")
  }

}
