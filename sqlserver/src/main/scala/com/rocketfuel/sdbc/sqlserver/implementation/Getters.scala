package com.rocketfuel.sdbc.sqlserver.implementation

import java.sql.SQLException
import java.time.{LocalTime, Instant, OffsetDateTime}
import java.util.UUID

import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.sqlserver.HierarchyId

import scala.xml.{Node, XML}

private[sdbc] trait Getters
  extends DefaultGetters
  with InstantGetter
  with LocalDateGetter
  with LocalDateTimeGetter {

  implicit val LocalTimeGetter: Getter[LocalTime] = new Getter[LocalTime] {
    override def apply(row: Row, ix: Index): Option[LocalTime] = {
      Option(row.getString(ix(row))).map(LocalTime.parse)
    }
  }

  implicit val OffsetDateTimeGetter: Getter[OffsetDateTime] = new Getter[OffsetDateTime] {
    override def apply(row: Row, ix: Index): Option[OffsetDateTime] = {
      Option(row.getString(ix(row))).map(s => OffsetDateTime.from(offsetDateTimeFormatter.parse(s)))
    }
  }

  override implicit val UUIDGetter: Getter[UUID] = new Parser[UUID] {
    override def parse(asString: String): UUID = {
      UUID.fromString(asString)
    }
  }

  implicit val HierarchyIdGetter = new Parser[HierarchyId] {
    override def parse(asString: String): HierarchyId = {
      HierarchyId.fromString(asString)
    }
  }

  implicit val XMLGetter: Getter[Node] = new Getter[Node] {

    override def apply(row: Row, ix: Index): Option[Node] = {
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
  }

  /**
   * The JTDS driver fails to parse timestamps, so when it fails, use our own parser.
   */
  override implicit val InstantGetter = new Getter[Instant] {
    override def apply(row: Row, ix: Index): Option[Instant] = {
      try {
        Option(row.getTimestamp(ix(row))).map(_.toInstant)
      } catch {
        case e: SQLException if e.getMessage.endsWith("cannot be converted to TIMESTAMP.") =>
          for {
            asString <- Option(row.getString(ix(row)))
          } yield {
            val parsed = offsetDateTimeFormatter.parse(asString)
            OffsetDateTime.from(parsed).toInstant
          }
      }
    }
  }

}
