package com.rocketfuel.sdbc.base.jdbc

import org.scalatest.FunSuite
import shapeless._
import shapeless.record._

/**
 * Test cases taken from https://github.com/tpolecat/doobie/blob/c8a273c365edf5a583621fbfd77a49297986d82f/core/src/test/scala/doobie/util/composite.scala
 */
class CompositeGetterSpec
  extends FunSuite
  with IntGetter
  with StringGetter
  with BooleanGetter
  with DoubleGetter
  with LongGetter {

  case class Woozle(a: (String, Int), b: Int :: String :: HNil, c: Boolean)

  test("CompositeGetter[Int]") {
    assertCompiles("CompositeGetter[Int]")
  }

  test("CompositeGetter[(Int, Int)]") {
    assertCompiles("CompositeGetter[(Int, Int)]")
  }

  test("CompositeGetter[(Int, Int, String)]") {
    assertCompiles("CompositeGetter[(Int, Int, String)]")
  }

  test("CompositeGetter[(Int, (Int, String))]") {
    assertCompiles("CompositeGetter[(Int, (Int, String))]")
  }

  test("CompositeGetter[Woozle]") {
    assertCompiles("CompositeGetter[Woozle]")
  }

  test("CompositeGetter[(Woozle, String)]") {
    assertCompiles("CompositeGetter[(Woozle, String)]")
  }

  test("CompositeGetter[(Int, Woozle :: Woozle :: String :: HNil)]") {
    assertCompiles("CompositeGetter[(Int, Woozle :: Woozle :: String :: HNil)]")
  }

  test("shapeless record") {
    type DL = (Double, Long)
    type A = Record.`'foo -> Int, 'bar -> String, 'baz -> DL, 'quz -> Woozle`.T

    CompositeGetter[A]
    CompositeGetter[(A, A)]
  }

}