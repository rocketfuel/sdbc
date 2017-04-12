package com.rocketfuel.sdbc.h2

import com.rocketfuel.sdbc.H2._
import scalaz.Scalaz._

class SetNullSpec
  extends H2Suite {

  test("Setting a parameter to NULL uses the correct index.") { implicit connection =>
    val result = Select[Option[Int]]("SELECT @param").on("param" -> none[Int]).one()

    assertResult(None)(result)
  }

}
