package com.rocketfuel.sdbc.postgresql.implementation

import com.rocketfuel.sdbc.base.jdbc.ParameterValue
import java.sql.PreparedStatement
import org.json4s.jackson.JsonMethods
import org.json4s.JValue
import org.postgresql.util.PGobject

private[sdbc] class PGJson(
  var jValue: Option[JValue] = None
) extends PGobject() {

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

private[sdbc] object PGJson {
  def apply(j: JValue): PGJson = {
    val p = new PGJson(jValue = Some(j))
    p
  }
}

private[sdbc] trait PGJsonParameter {
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
