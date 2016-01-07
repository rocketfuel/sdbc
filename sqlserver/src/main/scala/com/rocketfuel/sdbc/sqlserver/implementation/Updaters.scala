package com.rocketfuel.sdbc.sqlserver.implementation

import java.time.{LocalTime, OffsetDateTime}
import java.util.UUID

import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.sqlserver.HierarchyId

import scala.xml.Node

private[sdbc] trait Updaters
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
  with InstantUpdater
  with LocalDateUpdater {
  self: Updater
    with UpdatableRow
    with ParameterValue
    with MutableRow
    with OffsetDateTimeAsStringParameter =>

  implicit val LocalTimeUpdater = new Updater[LocalTime] {
    override def update(row: UpdatableRow, columnIndex: Int, x: LocalTime): Unit = {
      row.updateString(columnIndex, x.toString)
    }
  }

  implicit val OffsetDateTimeUpdater: Updater[OffsetDateTime] = new Updater[OffsetDateTime] {
    override def update(row: UpdatableRow, columnIndex: Int, x: OffsetDateTime): Unit = {
      row.updateString(columnIndex, offsetDateTimeFormatter.format(x))
    }
  }

  implicit val UUIDUpdater: Updater[UUID] = new Updater[UUID] {
    override def update(row: UpdatableRow, columnIndex: Int, x: UUID): Unit = {
      row.updateString(columnIndex, x.toString)
    }
  }

  implicit val HierarchyUpdater: Updater[HierarchyId] = new Updater[HierarchyId] {
    override def update(row: UpdatableRow, columnIndex: Int, x: HierarchyId): Unit = {
      row.updateString(columnIndex, x.toString)
    }
  }

  implicit val XmlUpdater: Updater[Node] = new Updater[Node] {
    override def update(row: UpdatableRow, columnIndex: Int, x: Node): Unit = {
      row.updateString(columnIndex, x.toString)
    }
  }

}
