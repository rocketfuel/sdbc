package com.rocketfuel.sdbc.h2.jdbc

class GenericGetterSpec extends H2Suite {

  test("(Int, Int)") {implicit connection =>
    val result = Select.generic[(Int, Int)]("VALUES (1, 2)").option()
    val expected = Some((1, 2))
    assertResult(expected)(result)
  }

}
