package com.rocketfuel.sdbc.postgresql

import org.json4s._
import org.json4s.jackson.JsonMethods._

class Json4SJacksonSupportSpec
  extends PostgreSqlSuite.Json4sJackson {

  import postgresql._

  test("Json interpolation works with ignore") {implicit connection =>
    val i: JValue = JLong(3L)
    val e = ignore"$i"

    assertResult(Map("0" -> ParameterValue.of(i)))(e.parameters)
  }

  testUpdate[JValue]("json")(parse("{}"))(parse("""{"a": 1}"""))

  val jsonString = """{"hi":"there"}"""

  testSelect[JValue](s"SELECT '$jsonString'::json", Some(parse(jsonString)))

  testSelect[JValue](s"SELECT '$jsonString'::jsonb", Some(parse(jsonString)))

}
