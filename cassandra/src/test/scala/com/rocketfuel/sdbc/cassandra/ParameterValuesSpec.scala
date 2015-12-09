package com.rocketfuel.sdbc.cassandra

import java.math.{BigDecimal => JBigDecimal}
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Date, UUID}
import java.util
import org.scalatest.FunSuite
import scodec.bits.ByteVector

class ParameterValuesSpec extends FunSuite {

  test("implicit Boolean conversion works") {
    assertCompiles("val _: ParameterValue = false")
  }

  test("implicit Boxed Boolean conversion works") {
    assertCompiles("val _: ParameterValue = java.lang.Boolean.valueOf(false)")
  }

  test("implicit ByteVector conversion works") {
    assertCompiles("val _: ParameterValue = ByteVector.empty")
  }

  test("implicit ByteBuffer conversion works") {
    assertCompiles("val _: ParameterValue = ByteBuffer.wrap(Array.emptyByteArray)")
  }

  test("implicit Array[Byte] conversion works") {
    assertCompiles("val _: ParameterValue = Array.emptyByteArray")
  }

  test("implicit Date conversion works") {
    assertCompiles("val _: ParameterValue = new Date(0)")
  }

  test("implicit Java BigDecimal conversion works") {
    assertCompiles("val _: ParameterValue = JBigDecimal.valueOf(0L)")
  }

  test("implicit Scala BigDecimal conversion works") {
    assertCompiles("val _: ParameterValue = BigDecimal(0L)")
  }

  test("implicit Double conversion works") {
    assertCompiles("val _: ParameterValue = 3.0")
  }

  test("implicit Boxed Double conversion works") {
    assertCompiles("val _: ParameterValue = java.lang.Double.valueOf(0.0)")
  }

  test("implicit Float conversion works") {
    assertCompiles("val _: ParameterValue = 3.0F")
  }

  test("implicit Boxed Float conversion works") {
    assertCompiles("val _: ParameterValue = java.lang.Float.valueOf(0.0F)")
  }

  test("implicit InetAddress conversion works") {
    assertCompiles("val _: ParameterValue = InetAddress.getByAddress(Array[Byte](127,0,0,1))")
  }

  test("implicit Int conversion works") {
    assertCompiles("val _: ParameterValue = 3")
  }

  test("implicit Boxed Int conversion works") {
    assertCompiles("val _: ParameterValue = java.lang.Integer.valueOf(0)")
  }

  test("implicit Seq conversion works") {
    assertCompiles("val _: ParameterValue = Seq.empty[Int]")
  }

  test("implicit java List conversion works") {
    assertCompiles("val _: ParameterValue = new util.LinkedList[Int]()")
  }

  test("implicit Long conversion works") {
    assertCompiles("val _: ParameterValue = 3L")
  }

  test("implicit Boxed Long conversion works") {
    assertCompiles("val _: ParameterValue = java.lang.Long.valueOf(0L)")
  }

  test("implicit java Map conversion works") {
    assertCompiles("val _: ParameterValue = new util.HashMap[String, String]()")
  }

  test("implicit Map conversion works") {
    assertCompiles("val _: ParameterValue = Map.empty[String, String]")
  }

  test("implicit Option[String] conversion works") {
    assertCompiles("val _: ParameterValue = Some(\"hello\")")
  }

  test("implicit scala.BigDecimal conversion works") {
    assertCompiles("val _: ParameterValue = BigDecimal(1)")
  }

  test("implicit java Set conversion works") {
    assertCompiles("val _: ParameterValue = new util.HashSet[String]()")
  }

  test("implicit Set conversion works") {
    assertCompiles("val _: ParameterValue = Set.empty[String]")
  }

  test("implicit String conversion works") {
    assertCompiles("val _: ParameterValue = \"\"")
  }

  test("implicit UUID conversion works") {
    assertCompiles("val _: ParameterValue = UUID.randomUUID()")
  }

  test("implicit Tuple2 conversion works") {
    assertCompiles("val _: ParameterValue = (1, 1)")
  }

  test("implicit Tuple3 conversion works") {
    assertCompiles("val _: ParameterValue = (1, 1, 1)")
  }

  test("implicit BigInteger conversion works") {
    assertCompiles("val _: ParameterValue = java.math.BigInteger.valueOf(0L)")
  }

}
