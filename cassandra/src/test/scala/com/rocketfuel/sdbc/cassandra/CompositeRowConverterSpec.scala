package com.rocketfuel.sdbc.cassandra

import org.scalatest.FunSuite
import shapeless._
import shapeless.record._

class CompositeRowConverterSpec extends FunSuite {

  case class Woozle(a: (String, Int), b: Int :: String :: HNil, c: Boolean)

  test("RowConverter[Int]") {
    assertCompiles("RowConverter[Int]")
  }

  test("RowConverter[(Int, Int)]") {
    assertCompiles("RowConverter[(Int, Int)]")
  }

  test("RowConverter[(Int, Int, String)]") {
    assertCompiles("RowConverter[(Int, Int, String)]")
  }

  test("RowConverter[(Int, (Int, String))]") {
    assertCompiles("RowConverter[(Int, (Int, String))]")
  }

  test("RowConverter[Woozle]") {
    assertCompiles("RowConverter[Woozle]")
  }

  test("RowConverter[(Woozle, String)]") {
    assertCompiles("RowConverter[(Woozle, String)]")
  }

  test("RowConverter[(Int, Woozle :: Woozle :: String :: HNil)]") {
    assertCompiles("RowConverter[(Int, Woozle :: Woozle :: String :: HNil)]")
  }

  test("shapeless record") {
    type DL = (Double, Long)
    type A = Record.`'foo -> Int, 'bar -> String, 'baz -> DL, 'quz -> Woozle`.T

    assertCompiles("RowConverter[A]")
    assertCompiles("RowConverter[(A, A)]")
  }

}
