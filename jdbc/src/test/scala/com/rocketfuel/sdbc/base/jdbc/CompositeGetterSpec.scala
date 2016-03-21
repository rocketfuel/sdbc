package com.rocketfuel.sdbc.base.jdbc

import org.scalatest.FunSuite
import shapeless._
import TestDbms._

/**
 * Test cases taken from https://github.com/tpolecat/doobie/blob/c8a273c365edf5a583621fbfd77a49297986d82f/core/src/test/scala/doobie/util/composite.scala
 */
class CompositeGetterSpec
  extends FunSuite
  with TypesForTesting {

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

  test("CompositeGetter[Row, Record]") {
    assertCompiles("CompositeGetter[Row, R]")
    assertCompiles("CompositeGetter[Row, (R, R)]")
  }
  
  test("CompositeGetter[ImmutableRow, Int]") {
    assertCompiles("CompositeGetter[ImmutableRow, Int]")
  }

  test("CompositeGetter[ImmutableRow, (Int, Int)]") {
    assertCompiles("CompositeGetter[ImmutableRow, (Int, Int)]")
  }

  test("CompositeGetter[ImmutableRow, (Int, Int, String)]") {
    assertCompiles("CompositeGetter[ImmutableRow, (Int, Int, String)]")
  }

  test("CompositeGetter[ImmutableRow, (Int, (Int, String))]") {
    assertCompiles("CompositeGetter[ImmutableRow, (Int, (Int, String))]")
  }

  test("CompositeGetter[ImmutableRow, Woozle]") {
    assertCompiles("CompositeGetter[ImmutableRow, Woozle]")
  }

  test("CompositeGetter[ImmutableRow, (Woozle, String)]") {
    assertCompiles("CompositeGetter[ImmutableRow, (Woozle, String)]")
  }

  test("CompositeGetter[ImmutableRow, (Int, Woozle :: Woozle :: String :: HNil)]") {
    assertCompiles("CompositeGetter[ImmutableRow, (Int, Woozle :: Woozle :: String :: HNil)]")
  }

  test("CompositeGetter[ImmutableRow, Record]") {
    assertCompiles("CompositeGetter[ImmutableRow, R]")
    assertCompiles("CompositeGetter[ImmutableRow, (R, R)]")
  }

  test("CompositeGetter[UpdatableRow, Int]") {
    assertCompiles("CompositeGetter[UpdatableRow, Int]")
  }

  test("CompositeGetter[UpdatableRow, (Int, Int)]") {
    assertCompiles("CompositeGetter[UpdatableRow, (Int, Int)]")
  }

  test("CompositeGetter[UpdatableRow, (Int, Int, String)]") {
    assertCompiles("CompositeGetter[UpdatableRow, (Int, Int, String)]")
  }

  test("CompositeGetter[UpdatableRow, (Int, (Int, String))]") {
    assertCompiles("CompositeGetter[UpdatableRow, (Int, (Int, String))]")
  }

  test("CompositeGetter[UpdatableRow, Woozle]") {
    assertCompiles("CompositeGetter[UpdatableRow, Woozle]")
  }

  test("CompositeGetter[UpdatableRow, (Woozle, String)]") {
    testNames
    assertCompiles("CompositeGetter[UpdatableRow, (Woozle, String)]")
  }

  test("CompositeGetter[UpdatableRow, (Int, Woozle :: Woozle :: String :: HNil)]") {
    assertCompiles("CompositeGetter[UpdatableRow, (Int, Woozle :: Woozle :: String :: HNil)]")
  }

  test("CompositeGetter[UpdatableRow, R]") {
    assertCompiles("CompositeGetter[UpdatableRow, R]")
    assertCompiles("CompositeGetter[UpdatableRow, (R, R)]")
  }

}
