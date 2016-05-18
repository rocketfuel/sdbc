package com.rocketfuel.sdbc.h2

import com.rocketfuel.sdbc.H2._

class CompositeGetterSpec extends H2Suite {

  test("Int") {implicit connection =>
    val query = Select[Int]("VALUES (1)", hasParameters = false)
    val result = query.option()(connection)
    val expected = Some(1)
    assertResult(expected)(result)
  }

  test("(Int, Int, Int)") {implicit connection =>
    val query = Select[(Int, Int, Int)]("VALUES (1, 2, 3)", hasParameters = false)
    val result = query.option()
    val expected = Some((1, 2, 3))
    assertResult(expected)(result)
  }

  test("(Int, String)") {implicit connection =>
    val query = Select[(Int, String)]("VALUES (1, 'hi')", hasParameters = false)
    val result = query.option()
    val expected = Some((1, "hi"))
    assertResult(expected)(result)
  }

  test("case class TestClass(id: Int, value: String)") {implicit connection =>
    case class TestClass(id: Int, value: String)

    val query = Select[TestClass]("VALUES (1, 'hi')", hasParameters = false)
    val result = query.option()
    val expected = Some(TestClass(1, "hi"))
    assertResult(expected)(result)
  }

}
