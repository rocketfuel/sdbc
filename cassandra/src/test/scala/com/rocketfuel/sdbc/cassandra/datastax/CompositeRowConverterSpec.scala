package com.rocketfuel.sdbc.cassandra.datastax

import com.rocketfuel.sdbc.cassandra.datastax.implementation.CompositeRowConverter
import com.rocketfuel.sdbc.cassandra.datastax.implementation.RowConverter
import org.scalatest.FunSuite
import shapeless._
import shapeless.record._

class CompositeRowConverterSpec extends FunSuite {

  case class Woozle(a: (String, Int), b: Int :: String :: HNil, c: Boolean)

  test("implicitly[RowConverter[Int]]") {
    implicitly[RowConverter[Int]]
  }

  test("CompositeRowConverter[(Int, Int)]") {
    implicitly[RowConverter[(Int, Int)]]
    assertCompiles("implicitly[RowConverter[(Int, Int)]]")
  }

  test("CompositeRowConverter[(Int, Int, String)]") {
    implicitly[RowConverter[(Int, Int, String)]]
    assertCompiles("implicitly[RowConverter[(Int, Int, String)]]")
  }

  test("CompositeRowConverter[(Int, (Int, String))]") {
    implicitly[RowConverter[(Int, (Int, String))]]
    assertCompiles("implicitly[RowConverter[(Int, (Int, String))]]")
  }

  test("CompositeRowConverter[Woozle]") {
    implicitly[RowConverter[Woozle]]
    assertCompiles("implicitly[RowConverter[[Woozle]]")
  }

  test("CompositeRowConverter[(Woozle, String)]") {
    implicitly[RowConverter[(Woozle, String)]]
    assertCompiles("implicitly[RowConverter[(Woozle, String)]]")
  }

  test("CompositeRowConverter[(Int, Woozle :: Woozle :: String :: HNil)]") {
    implicitly[RowConverter[(Int, Woozle :: Woozle :: String :: HNil)]]
    assertCompiles("implicitly[RowConverter[(Int, Woozle :: Woozle :: String :: HNil)]]")
  }

}
