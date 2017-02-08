package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.base.jdbc.resultset._
import java.net.InetAddress
import java.sql.{SQLDataException, SQLException}
import java.time.{Duration => JavaDuration, _}
import java.util.UUID
import argonaut._
import org.postgresql.util.{PGInterval, PGobject}
import scala.concurrent.duration.{Duration => ScalaDuration}
import scala.reflect.ClassTag

//PostgreSQL doesn't support Byte, so we don't use the default getters.
trait Getters
  extends BooleanGetter
  with BytesGetter
  with DateGetter
  with DoubleGetter
  with FloatGetter
  with IntGetter
  with JavaBigDecimalGetter
  with LongGetter
  with ScalaBigDecimalGetter
  with ShortGetter
  with StringGetter
  with TimeGetter
  with TimestampGetter
  with UUIDGetter
  with InstantGetter
  with LocalDateGetter
  with LocalDateTimeGetter
  with SeqGetter
  with XmlGetter {
  self: DBMS
    with IntervalImplicits =>

  implicit val LTreeGetter: Getter[LTree] =
    (row: Row, ix: Int) => {
      Option(row.getObject(ix)).map {
        case l: LTree => l
        case _ => throw new SQLException("column does not contain an ltree")
      }
    }

  implicit val CIdrGetter: Getter[Cidr] =
    (row: Row, ix: Int) => {
      Option(row.getObject(ix)).map {
        case p: Cidr => p
        case _ => throw new SQLException("column does not contain a cidr")
      }
    }

  def IsPGobjectGetter[A <: PGobject, B](converter: A => B)(implicit ctag: ClassTag[A]): Getter[B] =
    (row: Row, ix: Int) => {
      val shouldBePgValue = Option(row.getObject(ix))
      shouldBePgValue.map {
        case p: A =>
          converter(p)
        case p =>
          val actualType = row.getMetaData.getColumnTypeName(1)
          val actualJavaType = p.getClass.getName
          throw new SQLException(s"column is $actualType, returned as $actualJavaType, but ${ctag.runtimeClass.getName} was expected")
      }
    }

  implicit val PGIntervalGetter: Getter[PGInterval] = IsPGobjectGetter[PGInterval, PGInterval](identity)

  implicit val ScalaDurationGetter: Getter[ScalaDuration] = IsPGobjectGetter[PGInterval, ScalaDuration](PGIntervalToScalaDuration)

  implicit val JavaDurationGetter: Getter[JavaDuration] = IsPGobjectGetter[PGInterval, JavaDuration](PGIntervalToJavaDuration)

  implicit val InetAddressGetter: Getter[InetAddress] = IsPGobjectGetter[PGInetAddress, InetAddress](_.inetAddress.get)

  /*
  The PG driver uses Time and Timestamp for the following, even if we register custom types.
   */
//  implicit val LocalTimeGetter = IsPGobjectGetter[PGLocalTime, LocalTime](_.localTime.get)
//
//  implicit val OffsetTimeGetter = IsPGobjectGetter[PGTimeTz, OffsetTime](_.offsetTime.get)
//
//  implicit val OffsetDateTimeGetter = IsPGobjectGetter[PGTimestampTz, OffsetDateTime](_.offsetDateTime.get)

  implicit val LocalTimeGetter: Getter[LocalTime] =
    PGLocalTime.parse _

  implicit val OffsetTimeGetter: Getter[OffsetTime] =
    (value: String) => OffsetTime.from(offsetTimeFormatter.parse(value))

  implicit val OffsetDateTimeGetter: Getter[OffsetDateTime] =
    (value: String) => OffsetDateTime.from(offsetDateTimeFormatter.parse(value))

  override implicit val UUIDGetter: Getter[UUID] =
    (row: Row, ix: Int) => {
      Option(row.getObject(ix)).map {
        case uuid: UUID => uuid
        case _ => throw new SQLDataException("column does not contain a uuid")
      }
    }

  implicit val HStoreJavaGetter: Getter[java.util.Map[String, String]] = {
    (row: Row, ix: Int) =>
      Option(row.getObject(ix)).map {
        case m: java.util.Map[_, _] =>
          m.asInstanceOf[java.util.Map[String, String]]
        case _ =>
          throw new SQLException("column does not contain an hstore")
      }
  }

  implicit val HStoreScalaGetter: Getter[Map[String, String]] = {
    (row: Row, ix: Int) =>
      import scala.collection.JavaConverters._
      for {
        javaMap <- HStoreJavaGetter(row, ix)
      } yield javaMap.asScala.toMap
  }

}
