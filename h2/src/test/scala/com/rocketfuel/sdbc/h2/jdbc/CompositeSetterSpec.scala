package com.rocketfuel.sdbc.h2.jdbc

import H2._

class CompositeSetterSpec extends H2Suite {

  test("(Int, Int, Int)") { implicit connection =>
    val q = Select[(Int, Int, Int)]("VALUES (@a, @b, @c)")
    q.on(("a", (1, 2, 3)))
    assertResult(Some(1, 2, 3))(q.option())
  }

}
