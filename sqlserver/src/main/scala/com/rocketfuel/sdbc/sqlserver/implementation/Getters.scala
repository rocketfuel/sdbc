package com.rocketfuel.sdbc.sqlserver.implementation

import com.rocketfuel.sdbc.base.jdbc.resultset.DefaultGetters
import com.rocketfuel.sdbc.base.jdbc.statement.OffsetDateTimeAsStringParameter
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

  implicit val XmlGetterImmutable: Getter[Node] =
    (row: Row, ix: Index) =>
      Option(row.getString(ix(row))).map(XML.loadString)


  /**
   * The JTDS driver sometimes fails to parse timestamps, so we use our own parser.
   */
  override implicit val InstantGetter: Getter[Instant] = {
    (row: ConnectedRow, ix: Index) => {
      OffsetDateTimeGetter(row, ix).map(_.toInstant)
    }
  }

}
