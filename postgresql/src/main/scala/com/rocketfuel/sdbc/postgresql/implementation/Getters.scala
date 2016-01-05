package com.rocketfuel.sdbc.postgresql.implementation

import java.net.InetAddress
import java.sql.{SQLException, SQLDataException}
import java.time.format.DateTimeFormatter
import java.time.{Duration => JavaDuration, _}
import java.util.UUID
import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.postgresql.{LTree, Cidr}
import org.json4s.JValue
import org.postgresql.util.{PGInterval, PGobject}
import scala.xml.{Node, XML}
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
  with ParameterGetter
  with InstantGetter
  with LocalDateGetter
  with LocalDateTimeGetter {
  self: DBMS
    with PGTimestampTzImplicits
    with PGTimeTzImplicits
    with IntervalImplicits
    with PGInetAddressImplicits
    with PGJsonImplicits =>

  implicit val LTreeGetter: Getter[LTree] =
    (row: Row, ix: Index) => {
      Option(row.getObject(ix(row))).map {
        case l: LTree => l
        case _ => throw new SQLException("column does not contain an ltree")
      }
    }

  implicit val PGIntervalGetter: Getter[LTree] =
    (row: Row, ix: Index) => {
      Option(row.getObject(ix(row))).map {
        case p: PGInterval => p
        case _ => throw new SQLException("column does not contain an interval")
      }
    }

  implicit val CIdrGetter: Getter[Cidr] =
    (row: Row, ix: Index) => {
      Option(row.getObject(ix(row))).map {
        case p: Cidr => p
        case _ => throw new SQLException("column does not contain a cidr")
      }
    }

  private def IsPGobjectGetter[T](implicit converter: PGobject => T): Getter[T] =
    (row: Row, ix: Index) => {
      val shouldBePgValue = Option(row.getObject(ix(row)))
      shouldBePgValue.map {
        case p: PGobject =>
          converter(p)
        case _ =>
          throw new SQLException("column does not contain a PGobject")
      }
    }

  implicit val ScalaDurationGetter = IsPGobjectGetter[ScalaDuration]

  implicit val JavaDurationGetter = IsPGobjectGetter[JavaDuration]

  implicit val JValueGetter = IsPGobjectGetter[JValue]

  implicit val InetAddressGetter = IsPGobjectGetter[InetAddress]

  implicit val OffsetTimeGetter: Getter[OffsetTime] =
    (row: Row, ix: Index) => {
      for {
        asString <- Option(row.getString(ix(row)))
      } yield {
        val parsed = offsetTimeFormatter.parse(asString)
        OffsetTime.from(parsed)
      }
    }

  implicit val OffsetDateTimeGetter: Getter[OffsetDateTime] =
    (row: Row, ix: Index) => for {
      asString <- Option(row.getString(ix(row)))
    } yield {
      val parsed = offsetDateTimeFormatter.parse(asString)
      OffsetDateTime.from(parsed)
    }

  implicit val LocalTimeGetter: Getter[LocalTime] =
    (row: Row, ix: Index) => {
      for {
        asString <- Option(row.getString(ix(row)))
      } yield {
        val parsed = DateTimeFormatter.ISO_LOCAL_TIME.parse(asString)
        val l = LocalTime.from(parsed)
        l
      }
    }

  override implicit val UUIDGetter: Getter[UUID] =
    (row: Row, ix: Index) => {
      Option(row.getObject(ix(row))).map {
        case uuid: UUID => uuid
        case _ => throw new SQLDataException("column does not contain a uuid")
      }
    }

  implicit val XMLGetter: Getter[Node] =
    //PostgreSQL's ResultSet#getSQLXML just uses getString.
    (asString: String) => XML.loadString(asString)

  implicit val MapGetter: Getter[Map[String, String]] =
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
