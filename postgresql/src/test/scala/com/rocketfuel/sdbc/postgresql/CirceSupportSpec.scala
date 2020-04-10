package com.rocketfuel.sdbc.postgresql

import io.circe._

class CirceSupportSpec
  extends PostgreSqlSuite.Circe {

  import postgresql._

  test("Json interpolation works with ignore") {implicit connection =>
    val i: Json = Json.arr()
    val e = ignore"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  val jsonString = """{"hi":"there"}"""
  val json = Json.obj(("hi", Json.fromString("there")))

  testUpdate[Json]("json")(Json.obj())(json)

  testSelect[Json](s"SELECT '$jsonString'::json", Some(json))

  testSelect[Json](s"SELECT '$jsonString'::jsonb", Some(json))

}
