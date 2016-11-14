package com.rocketfuel.sdbc.postgresql

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.sql.Timestamp
import java.util.UUID
import com.rocketfuel.sdbc.PostgreSql._
import org.scalatest.FunSuite
import scodec.bits.ByteVector

class StringContextSpec extends FunSuite {

  test("Boolean interpolation works with ignore") {
    val b = false
    val e = ignore"$b"

    assertResult(Map("0" -> ParameterValue.of(b)))(e.parameters)
  }

  test("Boxed Boolean interpolation works with ignore") {
    val b: java.lang.Boolean = false
    val e = ignore"$b"

    assertResult(Map("0" -> ParameterValue.of(b.booleanValue())))(e.parameters)
  }

  test("Byte array interpolation works with ignore") {
    val b = Array[Byte](1,2,3)
    val e = ignore"$b"

    assertResult(Map("0" -> ParameterValue.of(b)))(e.parameters)
  }

  test("ByteBuffer interpolation works with ignore") {
    val a = Array[Byte](1, 2, 3)
    val b = ByteBuffer.wrap(a)
    val e = ignore"$b"

    assertResult(Map("0" -> ParameterValue.of(b)))(e.parameters)
  }

  test("ByteVector interpolation works with ignore") {
    val b = ByteVector(Vector[Byte](1, 2, 3))
    val e = ignore"$b"

    assertResult(Map("0" -> ParameterValue.of(b)))(e.parameters)
  }

  test("Scala BigDecimal interpolation works with ignore") {
    val d = BigDecimal(0)
    val e = ignore"$d"

    assertResult(Map("0" -> ParameterValue.of(d)))(e.parameters)
  }

  test("Java BigDecimal interpolation works with ignore") {
    val d = java.math.BigDecimal.valueOf(0L)
    val e = ignore"$d"

    assertResult(Map("0" -> ParameterValue.of(d)))(e.parameters)
  }

  test("Double interpolation works with ignore") {
    val i = 3.0
    val e = ignore"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("Boxed Double interpolation works with ignore") {
    val i: java.lang.Double = 3.0
    val e = ignore"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("Float interpolation works with ignore") {
    val i = 3.0F
    val e = ignore"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("Boxed Float interpolation works with ignore") {
    val i: java.lang.Float = 3.0F
    val e = ignore"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("Int interpolation works with ignore") {
    val i = 3
    val e = ignore"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("Boxed Int interpolation works with ignore") {
    val i: java.lang.Integer = 3
    val e = ignore"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("Long interpolation works with ignore") {
    val i = 3L
    val e = ignore"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("Boxed Long interpolation works with ignore") {
    val i: java.lang.Long = 3L
    val e = ignore"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("Short interpolation works with ignore") {
    val i = 3.toShort
    val e = ignore"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("Boxed Short interpolation works with ignore") {
    val i: java.lang.Short = 3.toShort
    val e = ignore"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  test("String interpolation works with ignore") {
    val s = "hi"
    val e = ignore"$s"

    assertResult(Map("0" -> ParameterValue.of(s)))(e.parameters)
  }

  test("Time interpolation works with ignore") {
    val t = new java.sql.Time(0)
    val e = ignore"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("Timestamp interpolation works with ignore") {
    val t = new Timestamp(0)
    val e = ignore"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("Reader interpolation works with ignore") {
    val t = new java.io.CharArrayReader(Array.emptyCharArray)
    val e = ignore"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("InputStream interpolation works with ignore") {
    val t = new ByteArrayInputStream(Array.emptyByteArray)
    val e = ignore"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("UUID interpolation works with ignore") {
    val t = UUID.randomUUID()
    val e = ignore"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("Instant interpolation works with ignore") {
    val t = java.time.Instant.now()
    val e = ignore"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("LocalDate interpolation works with ignore") {
    val t = java.time.LocalDate.now()
    val e = ignore"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("LocalTime interpolation works with ignore") {
    val t = java.time.LocalTime.now()
    val e = ignore"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("LocalDateTime interpolation works with ignore") {
    val t = java.time.LocalDateTime.now()
    val e = ignore"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("OffsetDateTime interpolation works with ignore") {
    val t = java.time.OffsetDateTime.now()
    val e = ignore"$t"

    assertResult(Map("0" -> ParameterValue.of(t)))(e.parameters)
  }

  test("OffsetTime interpolation works with ignore") {
    val t = java.time.OffsetTime.now()
    val e = ignore"$t"

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
