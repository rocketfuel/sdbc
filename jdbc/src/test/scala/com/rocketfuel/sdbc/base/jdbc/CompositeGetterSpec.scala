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
    type DL = (Int, String)
    type A = Record.`'foo -> Int, 'bar -> String, 'baz -> DL, 'quz -> Woozle`.T

    assertCompiles("CompositeGetter[A]")
    assertCompiles("CompositeGetter[(A, A)]")
  }

  test("ConnectedRow#apply works") {
    assertCompiles("val row: TestDbms.ConnectedRow = ???; val _ = row[String](???)")
  }

  test("ConnectedRow#apply works for optional value") {
    assertCompiles("val row: TestDbms.ConnectedRow = ???; val _ = row[Option[String]](???)")
  }

}
