package com.rocketfuel.sdbc.cassandra

import com.rocketfuel.sdbc.Cassandra
import java.math.{BigDecimal => JBigDecimal}
import java.net.InetAddress
import java.nio.ByteBuffer
import java.time.{Instant, LocalDate, LocalTime}
import java.util.{Date, UUID}
import java.util
import org.scalatest.FunSuite
import scodec.bits.ByteVector

class ParameterValuesSpec extends FunSuite {

  test("implicit Boolean conversion works") {
    assertCompiles("false: Cassandra.ParameterValue")
  }

  test("implicit Boxed Boolean conversion works") {
    assertCompiles("java.lang.Boolean.valueOf(false): Cassandra.ParameterValue")
  }

  test("implicit ByteVector conversion works") {
    assertCompiles("ByteVector.empty: Cassandra.ParameterValue")
  }

  test("implicit ByteBuffer conversion works") {
    assertCompiles("ByteBuffer.wrap(Array.emptyByteArray): Cassandra.ParameterValue")
  }

  test("implicit Array[Byte] conversion works") {
    assertCompiles("Array.emptyByteArray: Cassandra.ParameterValue")
  }

  test("implicit Instant conversion works") {
    assertCompiles("Instant.MIN: Cassandra.ParameterValue")
  }

  test("implicit LocalDate conversion works") {
    assertCompiles("LocalDate.MIN: Cassandra.ParameterValue")
  }

  test("implicit LocalTime conversion works") {
    assertCompiles("LocalTime.MIN: Cassandra.ParameterValue")
  }

  test("implicit Java BigDecimal conversion works") {
    assertCompiles("JBigDecimal.valueOf(0L): Cassandra.ParameterValue")
  }

  test("implicit Scala BigDecimal conversion works") {
    assertCompiles("BigDecimal(0L): Cassandra.ParameterValue")
  }

  test("implicit Double conversion works") {
    assertCompiles("3.0: Cassandra.ParameterValue")
  }

  test("implicit Boxed Double conversion works") {
    assertCompiles("java.lang.Double.valueOf(0.0): Cassandra.ParameterValue")
  }

  test("implicit Float conversion works") {
    assertCompiles("3.0F: Cassandra.ParameterValue")
  }

  test("implicit Boxed Float conversion works") {
    assertCompiles("java.lang.Float.valueOf(0.0F): Cassandra.ParameterValue")
  }

  test("implicit InetAddress conversion works") {
    assertCompiles("InetAddress.getByAddress(Array[Byte](127,0,0,1)): Cassandra.ParameterValue")
  }

  test("implicit Int conversion works") {
    assertCompiles("3: Cassandra.ParameterValue")
  }

  test("implicit Boxed Int conversion works") {
    assertCompiles("java.lang.Integer.valueOf(0): Cassandra.ParameterValue")
  }

  test("implicit Seq conversion works") {
    assertCompiles("Seq.empty[Int]: Cassandra.ParameterValue")
  }

  test("implicit java List conversion works") {
    assertCompiles("new util.LinkedList[Int](): Cassandra.ParameterValue")
  }

  test("implicit Long conversion works") {
    assertCompiles("3L: Cassandra.ParameterValue")
  }

  test("implicit Boxed Long conversion works") {
    assertCompiles("java.lang.Long.valueOf(0L): Cassandra.ParameterValue")
  }

  test("implicit java Map conversion works") {
    assertCompiles("new util.HashMap[String, String](): Cassandra.ParameterValue")
  }

  test("implicit Map conversion works") {
    assertCompiles("Map.empty[String, String]: Cassandra.ParameterValue")
  }

  test("implicit Option[String] conversion works") {
    assertCompiles("Some(\"hello\"): Cassandra.ParameterValue")
  }

  test("implicit scala.BigDecimal conversion works") {
    assertCompiles("BigDecimal(1): Cassandra.ParameterValue")
  }

  test("implicit java Set conversion works") {
    assertCompiles("new util.HashSet[String](): Cassandra.ParameterValue")
  }

  test("implicit Set conversion works") {
    assertCompiles("Set.empty[String]: Cassandra.ParameterValue")
  }

  test("implicit String conversion works") {
    assertCompiles("\"\": Cassandra.ParameterValue")
  }

  test("implicit UUID conversion works") {
    assertCompiles("UUID.randomUUID(): Cassandra.ParameterValue")
  }

  test("implicit Tuple2 conversion works") {
    assertCompiles("(1, 1): Cassandra.ParameterValue")
  }

  test("implicit Tuple3 conversion works") {
    assertCompiles("(1, 1, 1): Cassandra.ParameterValue")
  }

  test("implicit BigInteger conversion works") {
    assertCompiles("java.math.BigInteger.valueOf(0L): Cassandra.ParameterValue")
  }

  test("implicit None conversion works") {
    assertCompiles("None: Cassandra.ParameterValue")
  }

}
