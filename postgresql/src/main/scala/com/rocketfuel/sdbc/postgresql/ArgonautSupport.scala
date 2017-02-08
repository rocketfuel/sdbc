package com.rocketfuel.sdbc.postgresql

import argonaut._
import argonaut.Argonaut._
import org.postgresql.util.PGobject

private class PGArgonaut(
  var jValue: Option[Json]
) extends PGobject() {

  def this() {
    this(None)
  }

  setType("json")

  override def getValue: String = {
    jValue.map(_.nospaces).
    getOrElse(throw new IllegalStateException("setValue must be called first"))
  }

  override def setValue(value: String): Unit = {
    this.jValue = for {
      reallyValue <- Option(value)
    } yield {
      //PostgreSQL uses numeric (i.e. BigDecimal) for json numbers
      //http://www.postgresql.org/docs/9.4/static/datatype-json.html
      reallyValue.parseOption.get
    }
  }

}

private object PGArgonaut {
  implicit def apply(j: Json): PGArgonaut = {
    val p = new PGArgonaut(jValue = Some(j))
    p
  }
}

trait ArgonautSupport {
  self: PostgreSql =>

  implicit val JValueParameter: Parameter[Json] =
    (json: Json) => {
      val pgJson = PGArgonaut(json)
      (statement: PreparedStatement, ix: Int) => {
        statement.setObject(ix + 1, pgJson)
        statement
      }
    }

  implicit val jsonTypeName: ArrayTypeName[Json] =
    ArrayTypeName[Json]("json")

  implicit val JsonGetter: Getter[Json] = IsPGobjectGetter[PGArgonaut, Json](_.jValue.get)

  implicit val JValueUpdater: Updater[Json] =
    IsPGobjectUpdater[Json, PGArgonaut]

  override def initializeJson(connection: Connection): Unit = {
    connection.addDataType("json", classOf[PGArgonaut])
    connection.addDataType("jsonb", classOf[PGArgonaut])
  }

}
