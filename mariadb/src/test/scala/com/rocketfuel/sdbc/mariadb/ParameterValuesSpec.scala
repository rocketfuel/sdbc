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
    assertCompiles("false: MariaDb.ParameterValue")
  }

  test("implicit Boxed Boolean conversion works") {
    assertCompiles("java.lang.Boolean.valueOf(false): MariaDb.ParameterValue")
  }

  test("implicit ByteVector conversion works") {
    assertCompiles("ByteVector.empty: MariaDb.ParameterValue")
  }

  test("implicit ByteBuffer conversion works") {
    assertCompiles("ByteBuffer.wrap(Array.emptyByteArray): MariaDb.ParameterValue")
  }

  test("implicit Array[Byte] conversion works") {
    assertCompiles("Array.emptyByteArray: MariaDb.ParameterValue")
  }

  test("implicit java.util.Date conversion works") {
    assertCompiles("new util.Date(0L): MariaDb.ParameterValue")
  }

  test("implicit java.sql.Date conversion works") {
    assertCompiles("new java.sql.Date(0L): MariaDb.ParameterValue")
  }

  test("implicit Java BigDecimal conversion works") {
    assertCompiles("JBigDecimal.valueOf(0L): MariaDb.ParameterValue")
  }

  test("implicit Scala BigDecimal conversion works") {
    assertCompiles("BigDecimal(0L): MariaDb.ParameterValue")
  }

  test("implicit Double conversion works") {
    assertCompiles("3.0: MariaDb.ParameterValue")
  }

  test("implicit Boxed Double conversion works") {
    assertCompiles("java.lang.Double.valueOf(0.0): MariaDb.ParameterValue")
  }

  test("implicit Float conversion works") {
    assertCompiles("3.0F: MariaDb.ParameterValue")
  }

  test("implicit Boxed Float conversion works") {
    assertCompiles("java.lang.Float.valueOf(0.0F): MariaDb.ParameterValue")
  }

  test("implicit Int conversion works") {
    assertCompiles("3: MariaDb.ParameterValue")
  }

  test("implicit Boxed Int conversion works") {
    assertCompiles("java.lang.Integer.valueOf(0): MariaDb.ParameterValue")
  }

  test("implicit Long conversion works") {
    assertCompiles("3L: MariaDb.ParameterValue")
  }

  test("implicit Boxed Long conversion works") {
    assertCompiles("java.lang.Long.valueOf(0L): MariaDb.ParameterValue")
  }

  test("implicit Option[String] conversion works") {
    assertCompiles("Some(\"hello\"): MariaDb.ParameterValue")
  }

  test("implicit scala.BigDecimal conversion works") {
    assertCompiles("BigDecimal(1): MariaDb.ParameterValue")
  }

  test("implicit String conversion works") {
    assertCompiles("\"\": MariaDb.ParameterValue")
  }

  test("implicit None conversion works") {
    assertCompiles("None: MariaDb.ParameterValue")
  }

  test("implicit Instant conversion works") {
    assertCompiles("Instant.MIN: MariaDb.ParameterValue")
  }
  
}
