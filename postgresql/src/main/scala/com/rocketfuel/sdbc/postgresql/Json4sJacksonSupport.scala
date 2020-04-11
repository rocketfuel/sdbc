package com.rocketfuel.sdbc.postgresql

import org.json4s.JValue
import org.json4s.jackson.JsonMethods
import org.postgresql.util.PGobject

private class PGJson4sJackson(
  var jValue: Option[JValue]
) extends PGobject() {

  def this() {
    this(None)
  }

  setType("json")

  override def getValue: String = {
    jValue.map(j => JsonMethods.compact(JsonMethods.render(j))).
    getOrElse(throw new IllegalStateException("setValue must be called first"))
  }

  override def setValue(value: String): Unit = {
    this.jValue = for {
      reallyValue <- Option(value)
    } yield {
      //PostgreSQL uses numeric (i.e. BigDecimal) for json numbers
      //http://www.postgresql.org/docs/9.4/static/datatype-json.html
      JsonMethods.parse(reallyValue, useBigDecimalForDouble = true)
    }
  }

}

private object PGJson4sJackson {
  implicit def apply(j: JValue): PGJson4sJackson = {
    val p = new PGJson4sJackson(jValue = Some(j))
    p
  }
}

trait Json4sJacksonSupport {
  self: PostgreSql =>

  implicit val JValueParameter: Parameter[JValue] =
    (json: JValue) => {
      val pgJson = PGJson4sJackson(json)
      (statement: PreparedStatement, ix: Int) => {
        statement.setObject(ix + 1, pgJson)
        statement
      }
    }

  implicit val jsonTypeName: ArrayTypeName[JValue] =
    ArrayTypeName[JValue]("json")

  implicit val JValueGetter: Getter[JValue] = IsPGobjectGetter[PGJson4sJackson, JValue](_.jValue.get)

  implicit val JValueUpdater: Updater[JValue] =
    IsPGobjectUpdater[JValue, PGJson4sJackson]

  override def initializeJson(connection: Connection): Unit = {
    connection.addDataType("json", classOf[PGJson4sJackson])
    connection.addDataType("jsonb", classOf[PGJson4sJackson])
  }

}
