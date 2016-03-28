package com.rocketfuel.sdbc.postgresql.implementation

import com.rocketfuel.sdbc.base.jdbc.resultset._
import java.net.InetAddress
import java.sql.{SQLException, SQLDataException}
import java.time.{Duration => JavaDuration, _}
import java.util.UUID
import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.postgresql.{LTree, Cidr}
import org.json4s.JValue
import org.postgresql.util.{PGInterval, PGobject}
import scala.reflect.ClassTag
import scala.xml.{Elem, XML}
import scala.concurrent.duration.{Duration => ScalaDuration}

//PostgreSQL doesn't support Byte, so we don't use the default getters.
private[sdbc] trait Getters
  extends BooleanGetter
  with BytesGetter
  with DateGetter
  with DoubleGetter
  with FloatGetter
  with InputStreamGetter
  with IntGetter
  with JavaBigDecimalGetter
  with LongGetter
  with ReaderGetter
  with ScalaBigDecimalGetter
  with ShortGetter
  with StringGetter
  with TimeGetter
  with TimestampGetter
  with UUIDGetter
  with InstantGetter
  with LocalDateGetter
  with LocalDateTimeGetter
  with SeqGetter {
  self: DBMS
    with IntervalImplicits =>

  implicit val LTreeGetter: Getter[Row, LTree] =
    (row: Row, ix: Index) => {
      Option(row.getObject(ix(row))).map {
        case l: LTree => l
        case _ => throw new SQLException("column does not contain an ltree")
      }
    }

  implicit val CIdrGetter: Getter[Row, Cidr] =
    (row: Row, ix: Index) => {
      Option(row.getObject(ix(row))).map {
        case p: Cidr => p
        case _ => throw new SQLException("column does not contain a cidr")
      }
    }

  private def IsPGobjectGetter[A <: PGobject, B](converter: A => B)(implicit ctag: ClassTag[A]): Getter[Row, B] =
    (row: Row, ix: Index) => {
      val shouldBePgValue = Option(row.getObject(ix(row)))
      shouldBePgValue.map {
        case p: A =>
          converter(p)
        case p =>
          val actualType = row.getMetaData.getColumnTypeName(1)
          val actualJavaType = p.getClass.getName
          throw new SQLException(s"column is $actualType, returned as $actualJavaType, but ${ctag.runtimeClass.getName} was expected")
      }
    }

  implicit val PGIntervalGetter = IsPGobjectGetter[PGInterval, PGInterval](identity)

  implicit val ScalaDurationGetter = IsPGobjectGetter[PGInterval, ScalaDuration](PGIntervalToDuration)

  implicit val JavaDurationGetter = IsPGobjectGetter[PGInterval, JavaDuration](PGIntervalToJavaDuration)

  implicit val JValueGetter = IsPGobjectGetter[PGJson, JValue](_.jValue.get)

  implicit val InetAddressGetter = IsPGobjectGetter[PGInetAddress, InetAddress](_.inetAddress.get)

  /*
  The PG driver uses Time and Timestamp for the following, even if we register custom types.
   */
//  implicit val LocalTimeGetter = IsPGobjectGetter[PGLocalTime, LocalTime](_.localTime.get)
//
//  implicit val OffsetTimeGetter = IsPGobjectGetter[PGTimeTz, OffsetTime](_.offsetTime.get)
//
//  implicit val OffsetDateTimeGetter = IsPGobjectGetter[PGTimestampTz, OffsetDateTime](_.offsetDateTime.get)

  implicit val LocalTimeGetter: Getter[Row, LocalTime] =
    (value: String) => PGLocalTime.parse(value)

  implicit val OffsetTimeGetter: Getter[Row, OffsetTime] =
    (value: String) => OffsetTime.from(offsetTimeFormatter.parse(value))

  implicit val OffsetDateTimeGetter: Getter[Row, OffsetDateTime] =
    (value: String) => OffsetDateTime.from(offsetDateTimeFormatter.parse(value))

  override implicit val UUIDGetter: Getter[Row, UUID] =
    (row: Row, ix: Index) => {
      Option(row.getObject(ix(row))).map {
        case uuid: UUID => uuid
        case _ => throw new SQLDataException("column does not contain a uuid")
      }
    }

  implicit val XMLGetter: Getter[Row, Elem] =
    //PostgreSQL's ResultSet#getSQLXML just uses getString.
    (asString: String) => XML.loadString(asString)

  implicit val MapGetter: Getter[Row, Map[String, String]] =
    (row: Row, ix: Index) => {
      Option(row.getObject(ix(row))).map {
        case m: java.util.Map[_, _] =>
          import scala.collection.convert.decorateAsScala._
          val values =
            for (entry <- m.entrySet().asScala) yield {
              entry.getKey.asInstanceOf[String] -> entry.getValue.asInstanceOf[String]
            }
          Map(values.toSeq: _*)
        case _ =>
          throw new SQLException("column does not contain an hstore")
      }
    }

}
