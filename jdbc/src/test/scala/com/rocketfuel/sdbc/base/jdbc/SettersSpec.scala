package com.rocketfuel.sdbc.base.jdbc

import org.scalatest.FunSuite
import shapeless._
import shapeless.syntax.singleton._

class SettersSpec
  extends FunSuite {

  val q = TestDbms.Select[TestDbms.QueryResult.UpdateCount]("@hi @bye")

  val expectedParams =
    Map[String, TestDbms.ParameterValue](
      "hi" -> TestDbms.ParameterValue.of(3),
      "bye" -> TestDbms.ParameterValue.of(4)
    )

  test("set a pair") {
    val withParams = q.on("hi" -> 3, "bye" -> 4)

    assertResult(expectedParams)(withParams.parameterValues)
  }

  test("set a map") {
    val withParams = q.onParameters(expectedParams)

    assertResult(expectedParams)(withParams.parameterValues)
  }

  test("set a record") {
    val withParams = q.onRecord(('hi ->> 3) :: ('bye ->> 4) :: HNil)

    assertResult(expectedParams)(withParams.parameterValues)
  }

  test("set a product") {
    case class Args(hi: Int, bye: Int)

    val param = Args(3, 4)

    val withParams = q.onProduct(param)

    assertResult(expectedParams)(withParams.parameterValues)
  }

}
