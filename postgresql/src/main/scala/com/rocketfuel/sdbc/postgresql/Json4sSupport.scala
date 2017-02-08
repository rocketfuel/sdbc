package com.rocketfuel.sdbc.postgresql

import org.json4s.JValue
import org.json4s.jackson.JsonMethods
import org.postgresql.util.PGobject

private class PGJson4s(
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

private object PGJson4s {
  implicit def apply(j: JValue): PGJson4s = {
    val p = new PGJson4s(jValue = Some(j))
    p
  }
}

trait Json4sSupport {
  self: PostgreSql =>

  implicit val JValueParameter: Parameter[JValue] =
    (json: JValue) => {
      val pgJson = PGJson4s(json)
      (statement: PreparedStatement, ix: Int) => {
        statement.setObject(ix + 1, pgJson)
        statement
      }
    }

  implicit val jsonTypeName: ArrayTypeName[JValue] =
    ArrayTypeName[JValue]("json")

  implicit val JValueGetter: Getter[JValue] = IsPGobjectGetter[PGJson4s, JValue](_.jValue.get)

  implicit val JValueUpdater: Updater[JValue] =
    IsPGobjectUpdater[JValue, PGJson4s]

  override def initializeJson(connection: Connection): Unit = {
    connection.addDataType("json", classOf[PGJson4s])
    connection.addDataType("jsonb", classOf[PGJson4s])
  }

}
