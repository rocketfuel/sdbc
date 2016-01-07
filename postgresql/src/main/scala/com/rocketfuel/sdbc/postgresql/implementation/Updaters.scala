package com.rocketfuel.sdbc.postgresql.implementation

import java.net.InetAddress
import java.time.{Duration => JavaDuration, _}
import com.rocketfuel.sdbc.base.jdbc._
import org.json4s._
import org.postgresql.util.{PGInterval, PGobject}
import scala.concurrent.duration.{Duration => ScalaDuration}

//PostgreSQL doesn't support Byte, so we don't use the default updaters.
private[sdbc] trait Updaters
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

  private def IsPGobjectUpdater[A, B <: PGobject](implicit converter: A => B): Updater[A] = {
    new Updater[A] {
      override def update(row: UpdatableRow, columnIndex: Int, x: A): Unit = {
        PGobjectUpdater.update(row, columnIndex, converter(x))
      }
    }
  }

  implicit val OffsetTimeUpdater = IsPGobjectUpdater[OffsetTime, PGTimeTz]

  implicit val OffsetDateTimeUpdater = IsPGobjectUpdater[OffsetDateTime, PGTimestampTz]

  implicit val LocalTimeUpdater = IsPGobjectUpdater[LocalTime, PGLocalTime]

  implicit val ScalaDurationUpdater = IsPGobjectUpdater[ScalaDuration, PGInterval]

  implicit val JavaDurationUpdater = IsPGobjectUpdater[JavaDuration, PGInterval]

  implicit val JValueUpdater = IsPGobjectUpdater[JValue, PGJson]

  implicit val InetAddressUpdater = IsPGobjectUpdater[InetAddress, PGInetAddress]

  implicit val PGobjectUpdater = new Updater[PGobject] {
    override def update(
      row: UpdatableRow,
      columnIndex: Int,
      x: PGobject
    ): Unit = {
      row.updateObject(columnIndex, x)
    }
  }

  implicit val MapUpdater = new Updater[Map[String, String]] {
    override def update(row: UpdatableRow, columnIndex: Int, x: Map[String, String]): Unit = {
      import scala.collection.convert.decorateAsJava._
      row.updateObject(columnIndex, x.asJava)
    }
  }

}
