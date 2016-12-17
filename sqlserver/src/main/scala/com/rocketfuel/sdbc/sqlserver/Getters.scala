package com.rocketfuel.sdbc.sqlserver

import com.rocketfuel.sdbc.base.jdbc.resultset.DefaultGetters
import java.time._
import java.time.temporal.{ChronoField, TemporalAccessor}
import java.util.UUID
import scala.xml.{Node, XML}

trait Getters
  extends DefaultGetters {
  self: SqlServer =>

  override implicit val LocalTimeGetter: Getter[LocalTime] =
    (asString: String) => LocalTime.parse(asString)

  implicit val OffsetDateTimeGetter: Getter[OffsetDateTime] =
    (asString: String) => OffsetDateTime.from(offsetDateTimeFormatter.parse(asString))

  override implicit val UUIDGetter: Getter[UUID] =
    (asString: String) => UUID.fromString(asString)

  implicit val HierarchyIdGetter: Getter[HierarchyId] =
    (asString: String) => HierarchyId.fromString(asString)

  implicit val XmlGetterImmutable: Getter[Node] =
    (row: Row, ix: Int) =>
      Option(row.getString(ix)).map(XML.loadString)

  /**
   * JTDS sometimes fails to parse timestamps, so we use our own parser.
   */
  override implicit val InstantGetter: Getter[Instant] = { (asString: String) =>
    val parse: TemporalAccessor = instantFormatter.parse(asString)
    if (parse.isSupported(ChronoField.OFFSET_SECONDS))
      OffsetDateTime.from(parse).toInstant
    else Instant.from(parse)
  }

}
