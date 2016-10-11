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
import scodec.bits.ByteVector

trait Getter {
  self: DBMS =>

  /**
    * This is a DBMS specific type that creates getters from implicit methods
    * in its scope. The getter methods should go in the DBMS object.
    *
    * @tparam T
    */
  case class Getter[+T] private[sdbc] (getter: base.Getter[ConnectedRow, Index, T])
    extends base.Getter[ConnectedRow, Index, T] {

    override def apply(row: ConnectedRow, index: Index): Option[T] = {
      getter(row, index)
    }

  }

  object Getter {
    def apply[T](implicit getter: Getter[T]): Getter[T] = getter

    implicit def ofFunction[T](getter: base.Getter[ConnectedRow, Index, T]): Getter[T] = {
      Getter[T](getter)
    }

    implicit def ofParser[Row, T](parser: String => T)(implicit stringGetter: Getter[String]): Getter[T] = {
      Getter((row: ConnectedRow, index: Index) => stringGetter.getter(row, index).map(parser))
    }

    def ofVal[T <: AnyVal](valGetter: (ConnectedRow, Int) => T): Getter[T] = {
      (row: ConnectedRow, ix: Index) =>
        val value = valGetter(row, ix(row))
        if (row.wasNull) None
        else Some(value)
    }
  }

  trait Parser[+T] extends base.Getter[ConnectedRow, Index, T] {

    override def apply(row: ConnectedRow, index: Index): Option[T] = {
      Option(row.getString(index(row))).map(parse)
    }

    def parse(s: String): T
  }

}

trait LongGetter {
  self: DBMS =>

  implicit val LongGetter: Getter[Long] =
    Getter.ofVal[Long]((row, ix) => row.getLong(ix))

  implicit val BoxedLongGetter: Getter[lang.Long] = {
    (row: ConnectedRow, ix: Index) => LongGetter(row, ix).map(lang.Long.valueOf)
  }

}

trait IntGetter {
  self: DBMS =>

  implicit val IntGetter: Getter[Int] =
    Getter.ofVal[Int]((row, ix) => row.getInt(ix))

  implicit val BoxedIntegerGetter: Getter[lang.Integer] =
    (row: ConnectedRow, ix: Index) => IntGetter(row, ix).map(lang.Integer.valueOf)

}

trait ShortGetter {
  self: DBMS =>

  implicit val ShortGetter: Getter[Short] =
    Getter.ofVal[Short] { (row, ix) => row.getShort(ix) }

  implicit val BoxedShortGetter: Getter[lang.Short] = {
    (row: ConnectedRow, ix: Index) =>
      ShortGetter(row, ix).map(lang.Short.valueOf)
  }

}

trait ByteGetter {
  self: DBMS =>

  implicit val ByteGetter: Getter[Byte] =
    Getter.ofVal[Byte] { (row, ix) => row.getByte(ix) }

  implicit val BoxedByteGetter: Getter[lang.Byte] = {
    (row: ConnectedRow, ix: Index) =>
      ByteGetter(row, ix).map(lang.Byte.valueOf)
  }

}

trait BytesGetter {
  self: DBMS =>

  implicit val ArrayByteGetter: Getter[Array[Byte]] = {
    (row: ConnectedRow, ix: Index) =>
      Option(row.getBytes(ix(row)))
  }

  implicit val ByteBufferGetter: Getter[ByteBuffer] = {
    (row: ConnectedRow, ix: Index) =>
      ArrayByteGetter(row, ix).map(ByteBuffer.wrap)
  }

  implicit val ByteVectorGetter: Getter[ByteVector] = {
    (row: ConnectedRow, ix: Index) =>
      ArrayByteGetter(row, ix).map(ByteVector.apply)
  }

  implicit val SeqByteGetter: Getter[Seq[Byte]] = {
    (row: ConnectedRow, ix: Index) =>
      ArrayByteGetter(row, ix).map(_.toSeq)
  }

}

trait SeqGetter {
  self: DBMS
    with BytesGetter =>

  implicit def toSeqGetter[T](implicit getter: Getter[T]): Getter[Seq[T]] = {
    (row: ConnectedRow, ix: Index) =>
      for {
        a <- Option(row.getArray(ix(row)))
      } yield {
        val arrayIterator = ConnectedRow.iterator(a.getResultSet())
        val arrayValues = for {
          arrayRow <- arrayIterator
        } yield {
          arrayRow[T](1)
        }
        arrayValues.toVector
      }
  }

  implicit def toSeqOptionGetter[T](implicit getter: Getter[T]): Getter[Seq[Option[T]]] = {
    (row: ConnectedRow, ix: Index) =>
      for {
        a <- Option(row.getArray(ix(row)))
      } yield {
        val arrayIterator = ConnectedRow.iterator(a.getResultSet())
        val arrayValues = for {
          arrayRow <- arrayIterator
        } yield {
          arrayRow[Option[T]](1)
        }
        arrayValues.toVector
      }
  }

}

trait FloatGetter {
  self: DBMS =>

  implicit val FloatGetter: Getter[Float] =
    Getter.ofVal[Float] { (row, ix) => row.getFloat(ix) }

  implicit val BoxedFloatGetter: Getter[lang.Float] = {
    (row: ConnectedRow, ix: Index) =>
      FloatGetter(row, ix).map(lang.Float.valueOf)
  }

}

trait DoubleGetter {
  self: DBMS =>

  implicit val DoubleGetter: Getter[Double] =
    Getter.ofVal[Double]((row, ix) => row.getDouble(ix))

  implicit val BoxedDoubleGetter: Getter[lang.Double] =
    (row: ConnectedRow, ix: Index) =>
      DoubleGetter(row, ix).map(lang.Double.valueOf)
}

trait JavaBigDecimalGetter {
  self: DBMS =>

  implicit val JavaBigDecimalGetter: Getter[java.math.BigDecimal] =
    (row: ConnectedRow, ix: Index) => Option(row.getBigDecimal(ix(row)))

}

trait ScalaBigDecimalGetter {
  self: DBMS =>

  implicit val ScalaBigDecimalGetter: Getter[BigDecimal] = {
    (row: ConnectedRow, ix: Index) =>
      Option[BigDecimal](row.getBigDecimal(ix(row)))
  }

}

trait TimestampGetter {
  self: DBMS =>

  implicit val TimestampGetter: Getter[Timestamp] = {
    (row: ConnectedRow, ix: Index) =>
      Option(row.getTimestamp(ix(row)))
  }

}

trait DateGetter {
  self: DBMS =>

  implicit val DateGetter: Getter[Date] = {
    (row: ConnectedRow, ix: Index) =>
      Option(row.getDate(ix(row)))
  }

}

trait TimeGetter {
  self: DBMS =>

  implicit val TimeGetter: Getter[Time] = {
    (row: ConnectedRow, ix: Index) =>
      Option(row.getTime(ix(row)))
  }

}

trait LocalDateTimeGetter {
  self: DBMS =>

  implicit val LocalDateTimeGetter: Getter[LocalDateTime] =
    (row: ConnectedRow, ix: Index) =>
      Option(row.getTimestamp(ix(row))).map(_.toLocalDateTime)
}

trait InstantGetter {
  self: DBMS =>

  implicit val InstantGetter: Getter[Instant] =
    (row: ConnectedRow, ix: Index) =>
      Option(row.getTimestamp(ix(row))).map(_.toInstant)
}

trait LocalDateGetter {
  self: DBMS =>

  implicit val LocalDateGetter: Getter[LocalDate] =
    (row: ConnectedRow, ix: Index) =>
      Option(row.getDate(ix(row))).map(_.toLocalDate)

}

trait LocalTimeGetter {
  self: DBMS =>

  implicit val LocalTimeGetter: Getter[LocalTime] =
    (row: ConnectedRow, ix: Index) =>
      Option(row.getTime(ix(row))).map(_.toLocalTime)
}

trait BooleanGetter {
  self: DBMS =>

  implicit val BooleanGetter: Getter[Boolean] =
    Getter.ofVal[Boolean] { (row, ix) => row.getBoolean(ix) }

  implicit val BoxedBooleanGetter: Getter[lang.Boolean] =
    (row: ConnectedRow, ix: Index) =>
      BooleanGetter(row, ix).map(lang.Boolean.valueOf)
}

trait StringGetter {
  self: DBMS =>

  implicit val StringGetter: Getter[String] =
    (row: ConnectedRow, index: Index) =>
      Option(row.getString(index(row)))

}

trait UUIDGetter {
  self: DBMS =>

  implicit val UUIDGetter: Getter[UUID] =
    (row: ConnectedRow, ix: Index) =>
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

  implicit val URLGetter: Getter[URL] =
    (row: ConnectedRow, ix: Index) =>
      Option(row.getURL(ix(row)))

}
