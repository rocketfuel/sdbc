package com.rocketfuel.sdbc.base.jdbc

import org.scalatest.FunSuite
import shapeless._
import shapeless.syntax.singleton._
import scalaz.Scalaz._

class SettersSpec
  extends FunSuite {

  val q = TestDbms.Query[Unit]("@_1 @_2")

  test("set a pair") {
    assertCompiles("""q.on("_1" -> 3)""")
  }

  test("set a pair, and its values are correct") {
    val withParams = q.on("_1" -> 3)

    assertResult(Map("_1" -> TestDbms.ParameterValue.of(3)))(withParams.parameterValues)
  }

  test("set a record") {
    val params = ('_1 ->> 3) :: ('_2 ->> 4) :: HNil

    assertCompiles("""q.onRecord(params)""")
  }

  test("set a record, and its values are correct") {
    val params = ('_1 ->> 3) :: ('_2 ->> 4) :: HNil

    val withParams = q.onRecord(params)

    assertResult(Map(
      "_1" -> TestDbms.ParameterValue.of(3),
      "_2" -> TestDbms.ParameterValue.of(4)
    ))(withParams.parameterValues)
  }

  test("set a product") {
    case class Args(_1: Int, _2: Int)

    val param = Args(3, 4)

    assertCompiles("""q.onProduct(param)""")
  }

  test("set a product, and its values are correct") {
    case class Args(_1: Int, _2: Int)

    val param = Args(3, 4)

    val withParams = q.onProduct(param)

    assertResult(Map(
      "_1" -> TestDbms.ParameterValue.of(param._1),
      "_2" -> TestDbms.ParameterValue.of(param._2)
    ))(withParams.parameterValues)
  }

  test("set a tuple as a product") {
    val param = (1, 2)

    assertCompiles("q.onProduct(param)")
  }

  test("set a tuple as a product, and its values are correct") {
    val param = (1, 2)

    val withParams = q.onProduct(param)

    assertResult(Map[String, TestDbms.ParameterValue](
      "_1" -> param._1,
      "_2" -> param._2
    ))(withParams.parameterValues)
  }

}
