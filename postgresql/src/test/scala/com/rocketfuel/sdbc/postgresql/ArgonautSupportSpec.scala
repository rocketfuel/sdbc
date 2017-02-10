package com.rocketfuel.sdbc.postgresql

import argonaut._
import argonaut.Argonaut._

class ArgonautSupportSpec
  extends PostgreSqlSuite.Argonaut {

  test("Json interpolation works with ignore") {implicit connection =>
    val i: Json = 4L.jencode
    val e = ignore"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  testUpdate[Json]("json")("{}".parseOption.get)("""{"a": 1}""".parseOption.get)

  val jsonString = """{"hi":"there"}"""

  testSelect[Json](s"SELECT '$jsonString'::json", jsonString.parseOption)

  testSelect[Json](s"SELECT '$jsonString'::jsonb", jsonString.parseOption)

}
