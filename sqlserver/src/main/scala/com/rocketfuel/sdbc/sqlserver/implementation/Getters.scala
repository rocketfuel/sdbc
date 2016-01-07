package com.rocketfuel.sdbc.sqlserver.implementation

import java.time._
import java.util.UUID
import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.sqlserver.HierarchyId

import scala.xml.{Node, XML}

private[sdbc] trait Getters
  extends DefaultGetters {
  self: DBMS with OffsetDateTimeAsStringParameter =>

  override implicit val LocalTimeGetter: Getter[LocalTime] =
    (asString: String) => LocalTime.parse(asString)

  implicit val OffsetDateTimeGetter: Getter[OffsetDateTime] =
    (asString: String) => OffsetDateTime.from(offsetDateTimeFormatter.parse(asString))

  override implicit val UUIDGetter: Getter[UUID] =
    (asString: String) => UUID.fromString(asString)

  implicit val HierarchyIdGetter: Getter[HierarchyId] =
    (asString: String) => HierarchyId.fromString(asString)

  implicit val XMLGetter: Getter[Node] =
    (row: Row, ix: Index) => {
      row match {
        case row: MutableRow =>
          for {
            clob <- Option(row.getClob(ix(row)))
          } yield {
            val stream = clob.getCharacterStream()
            try {
              XML.load(stream)
            } finally {
              util.Try(stream.close())
            }
          }
        case _ =>
          Option(row.getString(ix(row))).map(XML.loadString)
      }
    }

  /**
   * The JTDS driver sometimes fails to parse timestamps, so we use our own parser.
   */
  override implicit val InstantGetter: Getter[Instant] = {
    (row: Row, ix: Index) => {
      OffsetDateTimeGetter(row, ix).map(_.toInstant)
    }
  }

}
