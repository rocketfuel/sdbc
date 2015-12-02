package com.rocketfuel.sdbc.h2

class CompositeSetterSpec extends H2Suite {

  test("(Int, Int, Int)") { implicit connection =>
    case class Param(a: Int, b: Int, c: Int)

    val expected = Param(1, 2, 3)

    val q = Select[Param]("VALUES (@a, @b, @c)").onProduct(expected)

    assertResult(Some(expected))(q.option())
  }

}
