package com.rocketfuel.sdbc.postgresql

import org.json4s.JValue
import org.json4s.native.JsonMethods
import org.postgresql.util.PGobject

private class PGJson4sNative(
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

private object PGJson4sNative {
  implicit def apply(j: JValue): PGJson4sNative = {
    val p = new PGJson4sNative(jValue = Some(j))
    p
  }
}

trait Json4sNativeSupport {
  self: PostgreSql =>

  implicit val JValueParameter: Parameter[JValue] =
    (json: JValue) => {
      val pgJson = PGJson4sNative(json)
      (statement: PreparedStatement, ix: Int) => {
        statement.setObject(ix + 1, pgJson)
        statement
      }
    }

  implicit val jsonTypeName: ArrayTypeName[JValue] =
    ArrayTypeName[JValue]("json")

  implicit val JValueGetter: Getter[JValue] = IsPGobjectGetter[PGJson4sNative, JValue](_.jValue.get)

  implicit val JValueUpdater: Updater[JValue] =
    IsPGobjectUpdater[JValue, PGJson4sNative]

  override def initializeJson(connection: Connection): Unit = {
    connection.addDataType("json", classOf[PGJson4sNative])
    connection.addDataType("jsonb", classOf[PGJson4sNative])
  }

}
