package com.rocketfuel.sdbc.mariadb

import com.rocketfuel.sdbc.MariaDb
import java.math.{BigDecimal => JBigDecimal}
import java.net.InetAddress
import java.nio.ByteBuffer
import java.time.Instant
import java.util
import org.scalatest.FunSuite
import scodec.bits.ByteVector

class ParameterValuesSpec
  extends FunSuite {

  test("implicit Boolean conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = false")
  }

  test("implicit Boxed Boolean conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = java.lang.Boolean.valueOf(false)")
  }

  test("implicit ByteVector conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = ByteVector.empty")
  }

  test("implicit ByteBuffer conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = ByteBuffer.wrap(Array.emptyByteArray)")
  }

  test("implicit Array[Byte] conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = Array.emptyByteArray")
  }

  test("implicit java.util.Date conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = new util.Date(0L)")
  }

  test("implicit java.sql.Date conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = new java.sql.Date(0L)")
  }

  test("implicit Java BigDecimal conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = JBigDecimal.valueOf(0L)")
  }

  test("implicit Scala BigDecimal conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = BigDecimal(0L)")
  }

  test("implicit Double conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = 3.0")
  }

  test("implicit Boxed Double conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = java.lang.Double.valueOf(0.0)")
  }

  test("implicit Float conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = 3.0F")
  }

  test("implicit Boxed Float conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = java.lang.Float.valueOf(0.0F)")
  }

  test("implicit Int conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = 3")
  }

  test("implicit Boxed Int conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = java.lang.Integer.valueOf(0)")
  }

  test("implicit Long conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = 3L")
  }

  test("implicit Boxed Long conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = java.lang.Long.valueOf(0L)")
  }

  test("implicit Option[String] conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = Some(\"hello\")")
  }

  test("implicit scala.BigDecimal conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = BigDecimal(1)")
  }

  test("implicit String conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = \"\"")
  }

  test("implicit None conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = None")
  }

  test("implicit Instant conversion works") {
    assertCompiles("val _: MariaDb.ParameterValue = Instant.MIN")
  }
  
}
