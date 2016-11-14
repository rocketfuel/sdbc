package com.rocketfuel.sdbc.base.jdbc

import TestDbms._
import org.scalatest.FunSuite
import scalaz.Scalaz._

class StringContextSpec
  extends FunSuite {

  test("empty string is identity") {
    val s = select""

    assertResult("")(s.queryText)
    assertResult("")(s.originalQueryText)
  }

  test("$i is replaced with ?") {
    val i = 3
    val s = select"$i"

    assertResult("?")(s.queryText)
    assertResult("@`0`")(s.originalQueryText)
    assertResult(1)(s.parameters.size)
    assertResult(ParameterValue.of(3))(s.parameters.head._2)
  }

  test("${i}hi is replaced with ?hi") {
    val i = 3
    val s = select"${i}hi"

    assertResult("?hi")(s.queryText)
    assertResult("@`0`hi")(s.originalQueryText)
    assertResult(1)(s.parameters.size)
    assertResult(ParameterValue.of(3))(s.parameters.head._2)
  }

  test("hi$i is replaced with hi?") {
    val i = 3
    val s = select"hi$i"

    assertResult("hi?")(s.queryText)
    assertResult("hi@`0`")(s.originalQueryText)
    assertResult(1)(s.parameters.size)
    assertResult(ParameterValue.of(3))(s.parameters.head._2)
  }

  test("hi${i}hi is replaced with hi?hi") {
    val i = 3
    val s = select"hi${i}hi"

    assertResult("hi?hi")(s.queryText)
    assertResult("hi@`0`hi")(s.originalQueryText)
    assertResult(1)(s.parameters.size)
    assertResult(ParameterValue.of(3))(s.parameters.head._2)
  }

  test("$i$i$i is replaced with ???") {
    val i = 3
    val j = 4
    val k = "hi"

    val s = select"$i$j$k"

    assertResult("???")(s.queryText)
    assertResult("@`0`@`1`@`2`")(s.originalQueryText)
    assertResult(3)(s.parameters.size)
    assertResult(Map[String, TestDbms.ParameterValue]("0" -> i, "1" -> j, "2" -> k))(s.parameters)
  }

  test("Execute interpolation works") {
    val i = 3
    val s = ignore"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(s.parameters)
  }

}
