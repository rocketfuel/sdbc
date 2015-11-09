package com.rocketfuel.sdbc.cassandra.datastax

import com.rocketfuel.sdbc.cassandra.datastax.implementation.RowConverter
import org.scalatest.FunSuite
import shapeless._
import shapeless.record._

class RowConverterSpec extends FunSuite {

  case class Woozle(a: (String, Int), b: Int :: String :: HNil, c: Boolean)

  test("RowConverter[Int]") {
    RowConverter[Int]
  }

  test("RowConverter[(Int, Int)]") {
    RowConverter[(Int, Int)]
  }

  test("RowConverter[(Int, Int, String)]") {
    RowConverter[(Int, Int, String)]
  }

  test("RowConverter[(Int, (Int, String))]") {
    RowConverter[(Int, (Int, String))]
  }

  test("RowConverter[Woozle]") {
    RowConverter[[Woozle]
  }

  test("RowConverter[(Woozle, String)]") {
    RowConverter[(Woozle, String)]
  }

  test("RowConverter[(Int, Woozle :: Woozle :: String :: HNil)]") {
    RowConverter[(Int, Woozle :: Woozle :: String :: HNil)]
  }

  test("shapeless record") {
    type DL = (Double, Long)
    type A = Record.`'foo -> Int, 'bar -> String, 'baz -> DL, 'quz -> Woozle`.T

    RowConverter[A]
    RowConverter[(A, A)]
  }

}
