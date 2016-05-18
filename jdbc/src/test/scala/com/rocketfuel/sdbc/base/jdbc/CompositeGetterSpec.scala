package com.rocketfuel.sdbc.base.jdbc

import org.scalatest.FunSuite
import shapeless._
import TestDbms._
import shapeless.record.Record

/**
 * Test cases taken from https://github.com/tpolecat/doobie/blob/c8a273c365edf5a583621fbfd77a49297986d82f/core/src/test/scala/doobie/util/composite.scala
 */
class CompositeGetterSpec
  extends FunSuite {

  case class Woozle(a: (String, Int), b: Int :: String :: HNil, c: Boolean)

  test("CompositeGetter[Row, Int]") {
    assertCompiles("CompositeGetter[Row, Int]")
  }

  test("CompositeGetter[Row, (Int, Int)]") {
    assertCompiles("CompositeGetter[Row, (Int, Int)]")
  }

  test("CompositeGetter[Row, (Int, Int, String)]") {
    assertCompiles("CompositeGetter[Row, (Int, Int, String)]")
  }

  test("CompositeGetter[Row, (Int, (Int, String))]") {
    assertCompiles("CompositeGetter[Row, (Int, (Int, String))]")
  }

  test("CompositeGetter[Row, Woozle]") {
    assertCompiles("CompositeGetter[Row, Woozle]")
  }

  test("CompositeGetter[Row, (Woozle, String)]") {
    assertCompiles("CompositeGetter[Row, (Woozle, String)]")
  }

  test("CompositeGetter[Row, (Int, Woozle :: Woozle :: String :: HNil)]") {
    assertCompiles("CompositeGetter[Row, (Int, Woozle :: Woozle :: String :: HNil)]")
  }

  test("shapeless record") {
    type DL = (Int, String)
    type A = Record.`'foo -> Int, 'bar -> String, 'baz -> DL, 'quz -> Woozle`.T

    assertCompiles("CompositeGetter[Row, A]")
    assertCompiles("CompositeGetter[Row, (A, A)]")
  }

}
