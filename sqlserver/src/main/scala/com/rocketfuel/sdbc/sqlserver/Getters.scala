package com.rocketfuel.sdbc.sqlserver

import com.rocketfuel.sdbc.base.jdbc.resultset.DefaultGetters
import java.time._
import java.time.temporal.{ChronoField, TemporalAccessor}
import java.util.UUID
import scala.xml._

trait Getters
  extends DefaultGetters {
  self: SqlServer =>

  override implicit val LocalTimeGetter: Getter[LocalTime] =
    LocalTime.parse _

  implicit val OffsetDateTimeGetter: Getter[OffsetDateTime] =
    (asString: String) => OffsetDateTime.from(offsetDateTimeFormatter.parse(asString))

  override implicit val UUIDGetter: Getter[UUID] =
    UUID.fromString _

  implicit val HierarchyIdGetter: Getter[HierarchyId] =
    HierarchyId.fromString _

  implicit val XmlNodeGetter: Getter[Node] =
    XML.loadString _

  implicit val XmlNodeSeqGetter: Getter[NodeSeq] = {
    (asString: String) =>
      val asDocument = "<xml>" + asString + "</xml>"
      NodeSeq.fromSeq(XML.loadString(asDocument).child)
  }

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
