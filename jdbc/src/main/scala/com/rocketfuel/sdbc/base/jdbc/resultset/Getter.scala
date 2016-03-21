package com.rocketfuel.sdbc.base.jdbc.resultset

import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.jdbc.DBMS
import java.io.{InputStream, Reader}
import java.lang
import java.net.URL
import java.nio.ByteBuffer
import java.sql.{Date, SQLException, Time, Timestamp}
import java.time._
import java.util.UUID
import scala.collection.generic.CanBuildFrom
import scodec.bits.ByteVector

trait Getter {
  self: DBMS =>

  trait Getter[-R <: Row, +T] extends base.Getter[R, Index, T]

  object Getter {

    def apply[R <: Row, T](implicit getter: Getter[R, T]): Getter[R, T] = getter

    implicit def ofFunction[R <: Row, T](getter: base.Getter[R, Index, T]): Getter[R, T] = {
      new Getter[R, T] {
        override def apply(v1: R, v2: Index): Option[T] = getter(v1, v2)
      }
    }

    implicit def ofParser[R <: Row, T](parser: String => T)(implicit stringGetter: Getter[R, String]): Getter[R, T] = {
      (row: R, index: Index) => stringGetter(row, index).map(parser)
    }

    def ofVal[R <: Row, T <: AnyVal](valGetter: (R, Int) => T): Getter[R, T] = {
      (row: R, ix: Index) =>
        val value = valGetter(row, ix(row))
        if (row.wasNull) None
        else Some(value)
    }

  }

  trait Parser[-R <: Row, +T] extends base.Getter[R, Index, T] {

    override def apply(row: R, index: Index): Option[T] = {
      Option(row.getString(index(row))).map(parse)
    }

    def parse(s: String): T
  }

}

trait LongGetter {
  self: DBMS =>

  implicit val LongGetter: Getter[Row, Long] =
    Getter.ofVal[Row, Long]((row, ix) => row.getLong(ix))

  implicit val BoxedLongGetter: Getter[Row, lang.Long] = {
    (row: Row, ix: Index) => LongGetter(row, ix).map(lang.Long.valueOf)
  }

}

trait IntGetter {
  self: DBMS =>

  implicit val IntGetter: Getter[Row, Int] =
    Getter.ofVal[Row, Int]((row, ix) => row.getInt(ix))

  implicit val BoxedIntegerGetter: Getter[Row, lang.Integer] =
    (row: Row, ix: Index) => IntGetter(row, ix).map(lang.Integer.valueOf)

}

trait ShortGetter {
  self: DBMS =>

  implicit val ShortGetter: Getter[Row, Short] =
    Getter.ofVal[Row, Short] { (row, ix) => row.getShort(ix) }

  implicit val BoxedShortGetter: Getter[Row, lang.Short] = {
    (row: Row, ix: Index) =>
      ShortGetter(row, ix).map(lang.Short.valueOf)
  }

}

trait ByteGetter {
  self: DBMS =>

  implicit val ByteGetter: Getter[Row, Byte] =
    Getter.ofVal[Row, Byte] { (row, ix) => row.getByte(ix) }

  implicit val BoxedByteGetter: Getter[Row, lang.Byte] = {
    (row: Row, ix: Index) =>
      ByteGetter(row, ix).map(lang.Byte.valueOf)
  }

}

trait BytesGetter {
  self: DBMS =>

  implicit val ArrayByteGetter: Getter[Row, Array[Byte]] = {
    (row: Row, ix: Index) =>
      Option(row.getBytes(ix(row)))
  }

  implicit val ByteBufferGetter: Getter[Row, ByteBuffer] = {
    (row: Row, ix: Index) =>
      ArrayByteGetter(row, ix).map(ByteBuffer.wrap)
  }

  implicit val ByteVectorGetter: Getter[Row, ByteVector] = {
    (row: Row, ix: Index) =>
      ArrayByteGetter(row, ix).map(ByteVector.apply)
  }

  implicit val SeqByteGetter: Getter[Row, Seq[Byte]] = {
    (row: Row, ix: Index) =>
      ArrayByteGetter(row, ix).map(_.toSeq)
  }

}

trait SeqGetter {
  self: DBMS =>

  implicit def canBuildFromGetter[F[_], T](
    implicit getter: CompositeGetter[Row, T],
    canBuildFrom: CanBuildFrom[Nothing, T, F[T]]
  ): Getter[Row, F[T]] = {
    (row: Row, ix: Index) =>
      val builder = canBuildFrom()
      for {
        a <- Option(row.getArray(ix(row)))
      } yield {
        val arrayIterator = ImmutableRow.iterator(a.getResultSet())
        val arrayValues = for {
          arrayRow <- arrayIterator
        } yield {
          arrayRow[T](1)
        }
        builder ++= arrayValues
        builder.result()
      }
  }

}

trait FloatGetter {
  self: DBMS =>

  implicit val FloatGetter: Getter[Row, Float] =
    Getter.ofVal[Row, Float] { (row, ix) => row.getFloat(ix) }

  implicit val BoxedFloatGetter: Getter[Row, lang.Float] = {
    (row: Row, ix: Index) =>
      FloatGetter(row, ix).map(lang.Float.valueOf)
  }

}

trait DoubleGetter {
  self: DBMS =>

  implicit val DoubleGetter: Getter[Row, Double] =
    Getter.ofVal[Row, Double]((row, ix) => row.getDouble(ix))

  implicit val BoxedDoubleGetter: Getter[Row, lang.Double] =
    (row: Row, ix: Index) =>
      DoubleGetter(row, ix).map(lang.Double.valueOf)
}

trait JavaBigDecimalGetter {
  self: DBMS =>

  implicit val JavaBigDecimalGetter: Getter[Row, java.math.BigDecimal] =
    (row: Row, ix: Index) => Option(row.getBigDecimal(ix(row)))

}

trait ScalaBigDecimalGetter {
  self: DBMS =>

  implicit val ScalaBigDecimalGetter: Getter[Row, BigDecimal] = {
    (row: Row, ix: Index) =>
      Option[BigDecimal](row.getBigDecimal(ix(row)))
  }

}

trait TimestampGetter {
  self: DBMS =>

  implicit val TimestampGetter: Getter[Row, Timestamp] = {
    (row: Row, ix: Index) =>
      Option(row.getTimestamp(ix(row)))
  }

}

trait DateGetter {
  self: DBMS =>

  implicit val DateGetter: Getter[Row, Date] = {
    (row: Row, ix: Index) =>
      Option(row.getDate(ix(row)))
  }

}

trait TimeGetter {
  self: DBMS =>

  implicit val TimeGetter: Getter[Row, Time] = {
    (row: Row, ix: Index) =>
      Option(row.getTime(ix(row)))
  }

}

trait LocalDateTimeGetter {
  self: DBMS =>

  implicit val LocalDateTimeGetter: Getter[Row, LocalDateTime] =
    (row: Row, ix: Index) =>
      Option(row.getTimestamp(ix(row))).map(_.toLocalDateTime)
}

trait InstantGetter {
  self: DBMS =>

  implicit val InstantGetter: Getter[Row, Instant] =
    (row: Row, ix: Index) =>
      Option(row.getTimestamp(ix(row))).map(_.toInstant)
}

trait LocalDateGetter {
  self: DBMS =>

  implicit val LocalDateGetter: Getter[Row, LocalDate] =
    (row: Row, ix: Index) =>
      Option(row.getDate(ix(row))).map(_.toLocalDate)

}

trait LocalTimeGetter {
  self: DBMS =>

  implicit val LocalTimeGetter: Getter[Row, LocalTime] =
    (row: Row, ix: Index) =>
      Option(row.getTime(ix(row))).map(_.toLocalTime)
}

trait BooleanGetter {
  self: DBMS =>

  implicit val BooleanGetter: Getter[Row, Boolean] =
    Getter.ofVal[Row, Boolean] { (row, ix) => row.getBoolean(ix) }

  implicit val BoxedBooleanGetter: Getter[Row, lang.Boolean] =
    (row: Row, ix: Index) =>
      BooleanGetter(row, ix).map(lang.Boolean.valueOf)
}

trait StringGetter {
  self: DBMS =>

  implicit val StringGetter: Getter[Row, String] =
    (row: Row, index: Index) =>
      Option(row.getString(index(row)))

}

trait UUIDGetter {
  self: DBMS =>

  implicit val UUIDGetter: Getter[Row, UUID] =
    (row: Row, ix: Index) =>
      Option(row.getObject(ix(row))).map {
        case u: UUID =>
          u
        case s: String =>
          UUID.fromString(s)
        case otherwise =>
          throw new SQLException("UUID value expected but not found.")
      }

}

//This is left out of the defaults, since no one seems to support it.
trait URLGetter {
  self: DBMS =>

  implicit val URLGetter: Getter[Row, URL] =
    (row: Row, ix: Index) =>
      Option(row.getURL(ix(row)))
}

trait InputStreamGetter {
  self: DBMS =>

  implicit val InputStreamGetter: Getter[UpdatableRow, InputStream] = {
    (row: UpdatableRow, ix: Index) =>
      Option(row.getBinaryStream(ix(row)))
  }

}

trait ReaderGetter {
  self: DBMS =>

  implicit val ReaderGetter: Getter[UpdatableRow, Reader] = {
    (row: UpdatableRow, ix: Index) =>
      Option(row.getCharacterStream(ix(row)))
  }

}
