package com.rocketfuel.sdbc.postgresql

import io.circe._
import io.circe.parser._
import org.postgresql.util.PGobject

private class PgCirce(
  var jValue: Option[Json]
) extends PGobject() {
  def this() {
    this(None)
  }

  setType("json")

  override def getValue: String = {
    jValue.map(_.noSpaces).
      getOrElse(throw new IllegalStateException("setValue must be called first"))
  }

  override def setValue(maybeValue: String): Unit = {
    this.jValue =
      for (value <- Option(maybeValue)) yield {
        parse(value) match {
          case Left(parseFailure) =>
            throw parseFailure
          case Right(result) =>
            result
        }
      }
  }
}

private object PgCirce {
  implicit def apply(j: Json): PgCirce = {
    new PgCirce(jValue = Some(j))
  }
}

trait CirceSupport {
  self: PostgreSql =>

  implicit val JsonParameter: Parameter[Json] =
    (json: Json) => {
      val pgJson = PgCirce(json)
      (statement: PreparedStatement, ix: Int) => {
        statement.setObject(ix + 1, pgJson)
        statement
      }
    }

  implicit val jsonTypeName: ArrayTypeName[Json] =
    ArrayTypeName[Json]("json")

  implicit val JsonGetter: Getter[Json] = IsPGobjectGetter[PgCirce, Json](_.jValue.get)

  implicit val JValueUpdater: Updater[Json] =
    IsPGobjectUpdater[Json, PgCirce]

  override def initializeJson(connection: Connection): Unit = {
    connection.addDataType("json", classOf[PgCirce])
    connection.addDataType("jsonb", classOf[PgCirce])
  }

}
