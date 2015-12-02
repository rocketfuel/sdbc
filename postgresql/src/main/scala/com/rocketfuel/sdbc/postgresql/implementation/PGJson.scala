package com.rocketfuel.sdbc.postgresql.implementation

import java.sql.SQLException

import com.rocketfuel.sdbc.base.ToParameter
import org.json4s.jackson.JsonMethods
import org.json4s.JValue
import org.postgresql.util.PGobject

private[sdbc] class PGJson() extends PGobject() {

  setType("json")

  var jValue: Option[JValue] = None

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

private[sdbc] object PGJson extends ToParameter {
  def apply(j: JValue): PGJson = {
    val p = new PGJson()
    p.jValue = Some(j)

    p
  }

  override val toParameter: PartialFunction[Any, Any] = {
    case j: JValue => PGJson(j)
  }
}

private[sdbc] trait PGJsonImplicits {

  implicit def JValueToPGJson(j: JValue): PGJson = {
    PGJson(j)
  }

  implicit def PGobjectToJValue(x: PGobject): JValue = {
    x match {
      case p: PGJson =>
        p.jValue.get
      case _ =>
        throw new SQLException("column does not contain a json")
    }
  }

}
