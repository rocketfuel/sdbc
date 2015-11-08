package com.rocketfuel.sdbc.cassandra.datastax

import com.rocketfuel.sdbc.cassandra.datastax.implementation.RowGetter
import com.rocketfuel.sdbc.cassandra.datastax.implementation.CompositeGetter
import org.scalatest.FunSuite
import shapeless._
import shapeless.record._

class CompositeGetterSpec extends FunSuite {

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

    assertCompiles("CompositeGetter[A]")
    assertCompiles("CompositeGetter[(A, A)]")
  }

}
