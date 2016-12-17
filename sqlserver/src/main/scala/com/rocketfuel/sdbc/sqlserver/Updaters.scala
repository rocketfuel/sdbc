package com.rocketfuel.sdbc.sqlserver

import com.rocketfuel.sdbc.base.jdbc._
import java.time._
import java.util.UUID
import scala.xml.Elem

trait Updaters
  extends LongUpdater
  with IntUpdater
  with ShortUpdater
  with ByteUpdater
  with BytesUpdater
  with DoubleUpdater
  with FloatUpdater
  with BigDecimalUpdater
  with TimestampUpdater
  with DateUpdater
  with TimeUpdater
  with BooleanUpdater
  with StringUpdater
  with InputStreamUpdater
  with ReaderUpdater
  with LocalDateTimeUpdater
  with LocalDateUpdater {
  self: SqlServer =>

  implicit val LocalTimeUpdater: Updater[LocalTime] =
    Updater.toString[LocalTime]

  implicit val InstantUpdater: Updater[Instant] =
    Updater.converted[Instant, String](instantFormatter.format)

  implicit val OffsetDateTimeUpdater: Updater[OffsetDateTime] =
    Updater.converted[OffsetDateTime, String](offsetDateTimeFormatter.format)

  implicit val UUIDUpdater: Updater[UUID] =
    Updater.toString[UUID]

  implicit val HierarchyUpdater: Updater[HierarchyId] =
    Updater.toString[HierarchyId]

  implicit val XmlElemUpdater: Updater[Elem] =
    Updater.toString[Elem]

}
