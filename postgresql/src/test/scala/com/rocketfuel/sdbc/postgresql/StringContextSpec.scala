package com.rocketfuel.sdbc.postgresql

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.sql.Timestamp
import java.util.UUID
import com.rocketfuel.sdbc.postgresql.implementation._
import org.scalatest.FunSuite
import scodec.bits.ByteVector

class StringContextSpec extends FunSuite {

  test("Boolean interpolation works with execute") {
    val b = false
    val e = execute"$b"

    assertResult(Map("0" -> Some(b)))(e.parameterValues)
  }

  test("Boxed Boolean interpolation works with execute") {
    val b: java.lang.Boolean = false
    val e = execute"$b"

    assertResult(Map("0" -> Some(b.booleanValue())))(e.parameterValues)
  }

  test("Byte array interpolation works with execute") {
    val b = Array[Byte](1,2,3)
    val e = execute"$b"

    assertResult(Map("0" -> Some(ByteVector(b))))(e.parameterValues)
  }

  test("ByteBuffer interpolation works with execute") {
    val a = Array[Byte](1, 2, 3)
    val b = ByteBuffer.wrap(a)
    val e = execute"$b"

    assertResult(Map("0" -> Some(ByteVector(a))))(e.parameterValues)
  }

  test("ByteVector interpolation works with execute") {
    val b = ByteVector(Vector[Byte](1, 2, 3))
    val e = execute"$b"

    assertResult(Map("0" -> Some(b)))(e.parameterValues)
  }

  test("java.util.Date interpolation works with execute") {
    val d = new java.util.Date(0)
    val e = execute"$d"

    assertResult(Map("0" -> Some(d)))(e.parameterValues)
  }

  test("Scala BigDecimal interpolation works with execute") {
    val d = BigDecimal(0)
    val e = execute"$d"

    assertResult(Map("0" -> Some(d.underlying)))(e.parameterValues)
  }

  test("Java BigDecimal interpolation works with execute") {
    val d = java.math.BigDecimal.valueOf(0L)
    val e = execute"$d"

    assertResult(Map("0" -> Some(d)))(e.parameterValues)
  }

  test("Double interpolation works with execute") {
    val i = 3.0
    val e = execute"$i"

    assertResult(Map("0" -> Some(i)))(e.parameterValues)
  }

  test("Boxed Double interpolation works with execute") {
    val i: java.lang.Double = 3.0
    val e = execute"$i"

    assertResult(Map("0" -> Some(i)))(e.parameterValues)
  }

  test("Float interpolation works with execute") {
    val i = 3.0F
    val e = execute"$i"

    assertResult(Map("0" -> Some(i)))(e.parameterValues)
  }

  test("Boxed Float interpolation works with execute") {
    val i: java.lang.Float = 3.0F
    val e = execute"$i"

    assertResult(Map("0" -> Some(i)))(e.parameterValues)
  }

  test("Int interpolation works with execute") {
    val i = 3
    val e = execute"$i"

    assertResult(Map("0" -> Some(i)))(e.parameterValues)
  }

  test("Boxed Int interpolation works with execute") {
    val i: java.lang.Integer = 3
    val e = execute"$i"

    assertResult(Map("0" -> Some(i)))(e.parameterValues)
  }

  test("Long interpolation works with execute") {
    val i = 3L
    val e = execute"$i"

    assertResult(Map("0" -> Some(i)))(e.parameterValues)
  }

  test("Boxed Long interpolation works with execute") {
    val i: java.lang.Long = 3L
    val e = execute"$i"

    assertResult(Map("0" -> Some(i)))(e.parameterValues)
  }

  test("Short interpolation works with execute") {
    val i = 3.toShort
    val e = execute"$i"

    assertResult(Map("0" -> Some(i)))(e.parameterValues)
  }

  test("Boxed Short interpolation works with execute") {
    val i: java.lang.Short = 3.toShort
    val e = execute"$i"

    assertResult(Map("0" -> Some(i)))(e.parameterValues)
  }

  test("String interpolation works with execute") {
    val s = "hi"
    val e = execute"$s"

    assertResult(Map("0" -> Some(s)))(e.parameterValues)
  }

  test("Time interpolation works with execute") {
    val t = new java.sql.Time(0)
    val e = execute"$t"

    assertResult(Map("0" -> Some(t)))(e.parameterValues)
  }

  test("Timestamp interpolation works with execute") {
    val t = new Timestamp(0)
    val e = execute"$t"

    assertResult(Map("0" -> Some(t)))(e.parameterValues)
  }

  test("Reader interpolation works with execute") {
    val t = new java.io.CharArrayReader(Array.emptyCharArray)
    val e = execute"$t"

    assertResult(Map("0" -> Some(t)))(e.parameterValues)
  }

  test("InputStream interpolation works with execute") {
    val t = new ByteArrayInputStream(Array.emptyByteArray)
    val e = execute"$t"

    assertResult(Map("0" -> Some(t)))(e.parameterValues)
  }

  test("UUID interpolation works with execute") {
    val t = UUID.randomUUID()
    val e = execute"$t"

    assertResult(Map("0" -> Some(t)))(e.parameterValues)
  }

  test("Instant interpolation works with execute") {
    val t = java.time.Instant.now()
    val e = execute"$t"

    assertResult(Map("0" -> Some(Timestamp.from(t))))(e.parameterValues)
  }

  test("LocalDate interpolation works with execute") {
    val t = java.time.LocalDate.now()
    val e = execute"$t"

    assertResult(Map("0" -> Some(java.sql.Date.valueOf(t))))(e.parameterValues)
  }

  test("LocalTime interpolation works with execute") {
    val t = java.time.LocalTime.now()
    val e = execute"$t"

    assertResult(Map("0" -> Some(PGLocalTime(t))))(e.parameterValues)
  }

  test("LocalDateTime interpolation works with execute") {
    val t = java.time.LocalDateTime.now()
    val e = execute"$t"

    assertResult(Map("0" -> Some(Timestamp.valueOf(t))))(e.parameterValues)
  }

  test("OffsetDateTime interpolation works with execute") {
    val t = java.time.OffsetDateTime.now()
    val e = execute"$t"

    assertResult(Map("0" -> Some(PGTimestampTz(t))))(e.parameterValues)
  }

  test("OffsetTime interpolation works with execute") {
    val t = java.time.OffsetTime.now()
    val e = execute"$t"

    assertResult(Map("0" -> Some(PGTimeTz(t))))(e.parameterValues)
  }

  test("Int interpolation works with select") {
    val i = 3
    val s = select"$i"

    assertResult(Map("0" -> Some(i)))(s.parameterValues)
  }

  test("Int interpolation works with update") {
    val i = 3
    val s = update"$i"

    assertResult(Map("0" -> Some(i)))(s.parameterValues)
  }

}
