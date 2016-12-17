package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.base.jdbc.statement._
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
  with SeqUpdater
  with XmlUpdater {
  self: DBMS
    with IntervalImplicits
    with SeqParameter =>

  def IsPGobjectUpdater[A, B <: PGobject](implicit converter: A => B): Updater[A] = {
    (row: UpdatableRow, columnIndex: Int, x: A) =>
      PGobjectUpdater(row, columnIndex, converter(x))
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

  implicit val PGobjectUpdater: Updater[PGobject] =
    _.updateObject(_, _)

  implicit val HStoreJavaUpdater: Updater[java.util.Map[String, String]] =
    _.updateObject(_, _)

  implicit val HStoreScalaUpdater: Updater[Map[String, String]] = {
    (row: UpdatableRow, columnIndex: Int, x: Map[String, String]) =>
      import scala.collection.JavaConverters._
      HStoreJavaUpdater(row, columnIndex, x.asJava)
    }

}
