package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.PostgreSql._
import org.scalatest.FunSuite

class UpdateSpec extends FunSuite {

  test("Identifier that appears as a word in the text can be assigned a value.") {
    val query = Select[UpdateCount]("SELECT * FROM tbl WHERE @t < t")

    assertResult(
      Map("t" -> ParameterValue.empty)
    )(
      query.on("t" -> None).parameterValues
    )
  }

  test("Identifier that doesn't appear as a word in the text can't be assigned a value.") {
    val query = Select[UpdateCount]("SELECT * FROM tbl WHERE @tt < t")

    intercept[IllegalArgumentException] {
      query.on("t" -> None)
    }
  }

}
