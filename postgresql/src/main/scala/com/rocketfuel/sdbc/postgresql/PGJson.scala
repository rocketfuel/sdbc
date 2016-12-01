package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.base.jdbc.statement.ParameterValue
import org.json4s.JValue
import org.json4s.jackson.JsonMethods
import org.postgresql.util.PGobject

private class PGJson(
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

private object PGJson {
  implicit def apply(j: JValue): PGJson = {
    val p = new PGJson(jValue = Some(j))
    p
  }
}

trait JValueParameter {
  self: ParameterValue =>

  implicit object JValueParameter extends Parameter[JValue] {
    override val set: JValue => (PreparedStatement, Int) => PreparedStatement = {
      json => (statement, ix) =>
        val pgJson = PGJson(json)
        statement.setObject(ix + 1, pgJson)
        statement
    }
  }

}
