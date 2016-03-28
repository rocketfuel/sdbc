package com.rocketfuel.sdbc.base.jdbc

import TestDbms._
import org.scalatest.FunSuite
import scalaz.Scalaz._

class StringContextSpec
  extends FunSuite {

  test("empty string is identity") {
    val s = query""

    assertResult("")(s.queryText)
    assertResult("")(s.originalQueryText)
  }

  test("$i is replaced with ?") {
    val i = 3
    val s = query"$i"

    assertResult("?")(s.queryText)
    assertResult("@`0`")(s.originalQueryText)
    assertResult(1)(s.parameterValues.size)
    assertResult(ParameterValue.of(3))(s.parameterValues.head._2)
  }

  test("${i}hi is replaced with ?hi") {
    val i = 3
    val s = query"${i}hi"

    assertResult("?hi")(s.queryText)
    assertResult("@`0`hi")(s.originalQueryText)
    assertResult(1)(s.parameterValues.size)
    assertResult(ParameterValue.of(3))(s.parameterValues.head._2)
  }

  test("hi$i is replaced with hi?") {
    val i = 3
    val s = query"hi$i"

    assertResult("hi?")(s.queryText)
    assertResult("hi@`0`")(s.originalQueryText)
    assertResult(1)(s.parameterValues.size)
    assertResult(ParameterValue.of(3))(s.parameterValues.head._2)
  }

  test("hi${i}hi is replaced with hi?hi") {
    val i = 3
    val s = query"hi${i}hi"

    assertResult("hi?hi")(s.queryText)
    assertResult("hi@`0`hi")(s.originalQueryText)
    assertResult(1)(s.parameterValues.size)
    assertResult(ParameterValue.of(3))(s.parameterValues.head._2)
  }

  test("$i$i$i is replaced with ???") {
    val i = 3
    val j = 4
    val k = "hi"

    val s = query"$i$j$k"

    assertResult("???")(s.queryText)
    assertResult("@`0`@`1`@`2`")(s.originalQueryText)
    assertResult(3)(s.parameterValues.size)
    assertResult(Map[String, TestDbms.ParameterValue]("0" -> i, "1" -> j, "2" -> k))(s.parameterValues)
  }

  test("Execute interpolation works") {
    val i = 3
    val s = execute"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(s.parameterValues)
  }

}
