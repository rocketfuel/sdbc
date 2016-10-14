package com.rocketfuel.sdbc.postgresql

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.sql.Timestamp
import java.util.UUID
import com.rocketfuel.sdbc.PostgreSql._
import org.scalatest.FunSuite
import scodec.bits.ByteVector

class StringContextSpec extends FunSuite {

  test("Boolean interpolation works with execute") {
    val b = false
    val e = execute"$b"

    assertResult(Map("0" -> ParameterValue.of(b)))(e.parameters)
  }

  test("Boxed Boolean interpolation works with execute") {
    val b: java.lang.Boolean = false
    val e = execute"$b"

    assertResult(Map("0" -> ParameterValue.of(b.booleanValue())))(e.parameters)
  }

  test("Byte array interpolation works with execute") {
    val b = Array[Byte](1,2,3)
    val e = execute"$b"

    assertResult(Map("0" -> ParameterValue.of(b)))(e.parameters)
  }

  test("ByteBuffer interpolation works with execute") {
    val a = Array[Byte](1, 2, 3)
    val b = ByteBuffer.wrap(a)
    val e = execute"$b"

    assertResult(Map("0" -> ParameterValue.of(b)))(e.parameters)
  }

  test("ByteVector interpolation works with execute") {
    val b = ByteVector(Vector[Byte](1, 2, 3))
    val e = execute"$b"

    assertResult(Map("0" -> ParameterValue.of(b)))(e.parameters)
  }

  test("Scala BigDecimal interpolation works with execute") {
    val d = BigDecimal(0)
    val e = execute"$d"

    assertResult(Map("0" -> ParameterValue.of(d)))(e.parameters)
  }

  test("Java BigDecimal interpolation works with execute") {
    val d = java.math.BigDecimal.valueOf(0L)
    val e = execute"$d"

    assertResult(Map("0" -> ParameterValue.of(d)))(e.parameters)
  }

  test("Double interpolation works with execute") {
    val i = 3.0
    val e = execute"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("Boxed Double interpolation works with execute") {
    val i: java.lang.Double = 3.0
    val e = execute"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("Float interpolation works with execute") {
    val i = 3.0F
    val e = execute"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("Boxed Float interpolation works with execute") {
    val i: java.lang.Float = 3.0F
    val e = execute"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("Int interpolation works with execute") {
    val i = 3
    val e = execute"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("Boxed Int interpolation works with execute") {
    val i: java.lang.Integer = 3
    val e = execute"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("Long interpolation works with execute") {
    val i = 3L
    val e = execute"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("Boxed Long interpolation works with execute") {
    val i: java.lang.Long = 3L
    val e = execute"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("Short interpolation works with execute") {
    val i = 3.toShort
    val e = execute"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("Boxed Short interpolation works with execute") {
    val i: java.lang.Short = 3.toShort
    val e = execute"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("String interpolation works with execute") {
    val s = "hi"
    val e = execute"$s"

    assertResult(Map("0" -> ParameterValue.of(s)))(e.parameters)
  }

  test("Time interpolation works with execute") {
    val t = new java.sql.Time(0)
    val e = execute"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("Timestamp interpolation works with execute") {
    val t = new Timestamp(0)
    val e = execute"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("Reader interpolation works with execute") {
    val t = new java.io.CharArrayReader(Array.emptyCharArray)
    val e = execute"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("InputStream interpolation works with execute") {
    val t = new ByteArrayInputStream(Array.emptyByteArray)
    val e = execute"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("UUID interpolation works with execute") {
    val t = UUID.randomUUID()
    val e = execute"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("Instant interpolation works with execute") {
    val t = java.time.Instant.now()
    val e = execute"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("LocalDate interpolation works with execute") {
    val t = java.time.LocalDate.now()
    val e = execute"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("LocalTime interpolation works with execute") {
    val t = java.time.LocalTime.now()
    val e = execute"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("LocalDateTime interpolation works with execute") {
    val t = java.time.LocalDateTime.now()
    val e = execute"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("OffsetDateTime interpolation works with execute") {
    val t = java.time.OffsetDateTime.now()
    val e = execute"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("OffsetTime interpolation works with execute") {
    val t = java.time.OffsetTime.now()
    val e = execute"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("Int interpolation works with select") {
    val i = 3
    val s = select"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(s.parameters)
  }

  test("Int interpolation works with update") {
    val i = 3
    val s = update"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(s.parameters)
  }

}
