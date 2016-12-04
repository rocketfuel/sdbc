package com.rocketfuel.sdbc.base

import org.scalatest.FunSuite

class CompiledStatementSpec extends FunSuite {

  test("Example parameter name @hello works.") {
    val statement = CompiledStatement("@hello")
    assert(statement.parameterPositions == Map("hello" -> Set(0)))
  }

  test("Example parameter name @`hello there` works.") {
    val statement = CompiledStatement("@`hello there`")
    assert(statement.parameterPositions == Map("hello there" -> Set(0)))
  }

  test("Example parameter name @_i_am_busy works.") {
    val statement = CompiledStatement("@_i_am_busy")
    assert(statement.parameterPositions == Map("_i_am_busy" -> Set(0)))
  }

  test("No parameters cause no change in the query") {
    val queryText = "SELECT * FROM tbl"
    val statement = CompiledStatement(queryText)

    assert(statement.queryText == queryText)
  }

  test("One parameter causes one '?'") {
    val queryText = "SELECT * FROM tbl WHERE @t = t"
    val queryResult = "SELECT * FROM tbl WHERE ? = t"

    val statement = CompiledStatement(queryText)

    assert(statement.queryText == queryResult)
  }

  test("One quoted parameter causes one '?'") {
    val queryText = "SELECT * FROM tbl WHERE @`t` = t"
    val queryResult = "SELECT * FROM tbl WHERE ? = t"

    val statement = CompiledStatement(queryText)

    assert(statement.queryText == queryResult)
  }

  test("One parameter used twice causes two '?'") {
    val queryText = "SELECT * FROM tbl WHERE @t < t0 AND @t > t1"
    val queryResult = "SELECT * FROM tbl WHERE ? < t0 AND ? > t1"

    val statement = CompiledStatement(queryText)

    assert(statement.queryText == queryResult)
  }

  test("One quoted parameter used twice causes two '?'") {
    val queryText = "SELECT * FROM tbl WHERE @`t` < t0 AND @`t` > t1"
    val queryResult = "SELECT * FROM tbl WHERE ? < t0 AND ? > t1"

    val statement = CompiledStatement(queryText)

    assert(statement.queryText == queryResult)
  }

  test("One parameter used twice has two parameter indexes") {
    val queryText = "SELECT * FROM tbl WHERE @t < t0 AND @t > t1 OR @t12"

    val statement = CompiledStatement(queryText)

    assert(statement.parameterPositions("t") == Set(0, 1))
  }

  test("One quoted parameter used twice has two parameter indexes") {
    val queryText = "SELECT * FROM tbl WHERE @`t` < t0 AND @`t` > t1 OR @t12"

    val statement = CompiledStatement(queryText)

    assert(statement.parameterPositions("t") == Set(0, 1))
  }

  test("Two parameters used once each get one parameter index each") {
    val queryText = "SELECT * FROM tbl where x BETWEEN @start AND @end OR y BETWEEN @start AND @end"

    val statement = CompiledStatement(queryText)

    assert(statement.parameterPositions("start") == Set(0, 2))

    assert(statement.parameterPositions("end") == Set(1, 3))
  }

  test("Two quoted parameters used once each get one parameter index each") {
    val queryText = "SELECT * FROM tbl where x BETWEEN @`start` AND @`end` OR y BETWEEN @`start` AND @`end`"

    val statement = CompiledStatement(queryText)

    assert(statement.parameterPositions("start") == Set(0, 2))

    assert(statement.parameterPositions("end") == Set(1, 3))
  }

  test("Recognizes non-latin character in identifier") {
    val statement = CompiledStatement("SELECT * FROM tbl WHERE @音 = 3")

    assert(statement.parameterPositions.contains("音"))
  }

  test("Symbols aren't considered valid identifier characters") {
    val statement = CompiledStatement("SELECT @t/3")

    assert(statement.parameterPositions.contains("t"))
  }

  test("Two '@' in a row are a single '@'.") {
    val queryText = "@@hello"
    val statement = CompiledStatement(queryText)

    assert(statement.parameterPositions.isEmpty)
    assertResult("@hello")(statement.queryText)
  }

  test("Two '@' in a row and then a parameter is a single '@' and the parameter.") {
    val queryText = "@@@hello"
    val statement = CompiledStatement(queryText)

    assertResult(Set(0))(statement.parameterPositions("hello"))
    assertResult("@?")(statement.queryText)
  }

  test("An even number of '@' yields half as many literal '@'") {
    val queryText = Vector.fill(10)("@")
    val statement = CompiledStatement(queryText.mkString)

    assertResult(Vector.fill(5)("@").mkString)(statement.queryText)
  }

  test("An uneven number of '@' yields an exception.") {
    val queryText = Vector.fill(7)("@")
    intercept[IllegalStateException](CompiledStatement(queryText.mkString))
  }

}
