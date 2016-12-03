package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.base.jdbc._
import java.net.InetAddress
import java.time.{Duration => JavaDuration, _}
import org.json4s._
import org.postgresql.util.{PGInterval, PGobject}
import scala.concurrent.duration.{Duration => ScalaDuration}

//PostgreSQL doesn't support Byte, so we don't use the default updaters.
trait Updaters
  extends LongUpdater
  with IntUpdater
  with ShortUpdater
  with BytesUpdater
  with DoubleUpdater
  with FloatUpdater
  with BigDecimalUpdater
  with TimestampUpdater
  with DateUpdater
  with TimeUpdater
  with BooleanUpdater
  with StringUpdater
  with UUIDUpdater
  with InputStreamUpdater
  with ReaderUpdater
  with LocalDateTimeUpdater
  with InstantUpdater
  with LocalDateUpdater
  with XmlUpdater {
  self: DBMS
    with IntervalImplicits =>

  def IsPGobjectUpdater[A, B <: PGobject](implicit converter: A => B): Updater[A] = {
    new Updater[A] {
      override def update(row: UpdatableRow, columnIndex: Int, x: A): Unit = {
        PGobjectUpdater.update(row, columnIndex, converter(x))
      }
    }
  }

  implicit val OffsetTimeUpdater: Updater[OffsetTime] =
    IsPGobjectUpdater[OffsetTime, PGTimeTz]

  implicit val OffsetDateTimeUpdater: Updater[OffsetDateTime] =
    IsPGobjectUpdater[OffsetDateTime, PGTimestampTz]

  implicit val LocalTimeUpdater: Updater[LocalTime] =
    IsPGobjectUpdater[LocalTime, PGLocalTime]

  implicit val ScalaDurationUpdater: Updater[ScalaDuration] =
    IsPGobjectUpdater[ScalaDuration, PGInterval]

  implicit val JavaDurationUpdater: Updater[JavaDuration] =
    IsPGobjectUpdater[JavaDuration, PGInterval]

  implicit val JValueUpdater: Updater[JValue] =
    IsPGobjectUpdater[JValue, PGJson]

  implicit val InetAddressUpdater: Updater[InetAddress] =
    IsPGobjectUpdater[InetAddress, PGInetAddress]

  implicit val PGobjectUpdater: Updater[PGobject] = new Updater[PGobject] {
    override def update(
      row: UpdatableRow,
      columnIndex: Int,
      x: PGobject
    ): Unit = {
      row.updateObject(columnIndex, x)
    }
  }

  implicit val MapUpdater: Updater[Map[String, String]] = new Updater[Map[String, String]] {
    override def update(row: UpdatableRow, columnIndex: Int, x: Map[String, String]): Unit = {
      import scala.collection.JavaConverters._
      row.updateObject(columnIndex, x.asJava)
    }
  }

}
