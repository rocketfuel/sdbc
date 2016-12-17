package com.rocketfuel.sdbc.base.jdbc.resultset

import com.rocketfuel.sdbc.base.jdbc.DBMS
import java.lang
import java.net.URL
import java.nio.ByteBuffer
import java.sql.{Date, SQLException, Time, Timestamp}
import java.time._
import java.util.UUID
import scodec.bits.ByteVector

trait LongGetter {
  self: DBMS =>

  implicit val LongGetter: Getter[Long] =
    ofVal[Long]((row, ix) => row.getLong(ix))

  implicit val BoxedLongGetter: Getter[lang.Long] = {
    (row: Row, ix: Int) => LongGetter(row, ix).map(lang.Long.valueOf)
  }

}

trait IntGetter {
  self: DBMS =>

  implicit val IntGetter: Getter[Int] =
    ofVal[Int]((row, ix) => row.getInt(ix))

  implicit val BoxedIntegerGetter: Getter[lang.Integer] =
    (row: ConnectedRow, ix: Int) => IntGetter(row, ix).map(lang.Integer.valueOf)

}

trait ShortGetter {
  self: DBMS =>

  implicit val ShortGetter: Getter[Short] =
    ofVal[Short] { (row, ix) => row.getShort(ix) }

  implicit val BoxedShortGetter: Getter[lang.Short] = {
    (row: ConnectedRow, ix: Int) =>
      ShortGetter(row, ix).map(lang.Short.valueOf)
  }

}

trait ByteGetter {
  self: DBMS =>

  implicit val ByteGetter: Getter[Byte] =
    ofVal[Byte] { (row, ix) => row.getByte(ix) }

  implicit val BoxedByteGetter: Getter[lang.Byte] = {
    (row: ConnectedRow, ix: Int) =>
      ByteGetter(row, ix).map(lang.Byte.valueOf)
  }

}

trait BytesGetter {
  self: DBMS =>

  implicit val ArrayByteGetter: Getter[Array[Byte]] = {
    (row: ConnectedRow, ix: Int) =>
      Option(row.getBytes(ix))
  }

  implicit val ByteBufferGetter: Getter[ByteBuffer] = {
    (row: ConnectedRow, ix: Int) =>
      ArrayByteGetter(row, ix).map(ByteBuffer.wrap)
  }

  implicit val ByteVectorGetter: Getter[ByteVector] = {
    (row: ConnectedRow, ix: Int) =>
      ArrayByteGetter(row, ix).map(ByteVector.apply)
  }

  implicit val SeqByteGetter: Getter[Seq[Byte]] = {
    (row: ConnectedRow, ix: Int) =>
      ArrayByteGetter(row, ix).map(_.toSeq)
  }

}

trait SeqGetter {
  self: DBMS
    with BytesGetter =>

  implicit def toSeqGetter[T](implicit getter: Getter[T]): Getter[Seq[T]] = {
    (row: ConnectedRow, ix: Int) =>
      for {
        a <- Option(row.getArray(ix))
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
    (row: ConnectedRow, ix: Int) =>
      for {
        a <- Option(row.getArray(ix))
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
    ofVal[Float] { (row, ix) => row.getFloat(ix) }

  implicit val BoxedFloatGetter: Getter[lang.Float] = {
    (row: ConnectedRow, ix: Int) =>
      FloatGetter(row, ix).map(lang.Float.valueOf)
  }

}

trait DoubleGetter {
  self: DBMS =>

  implicit val DoubleGetter: Getter[Double] =
    ofVal[Double]((row, ix) => row.getDouble(ix))

  implicit val BoxedDoubleGetter: Getter[lang.Double] =
    (row: ConnectedRow, ix: Int) =>
      DoubleGetter(row, ix).map(lang.Double.valueOf)
}

trait JavaBigDecimalGetter {
  self: DBMS =>

  implicit val JavaBigDecimalGetter: Getter[java.math.BigDecimal] =
    (row: ConnectedRow, ix: Int) => Option(row.getBigDecimal(ix))

}

trait ScalaBigDecimalGetter {
  self: DBMS =>

  implicit val ScalaBigDecimalGetter: Getter[BigDecimal] = {
    (row: ConnectedRow, ix: Int) =>
      Option[BigDecimal](row.getBigDecimal(ix))
  }

}

trait TimestampGetter {
  self: DBMS =>

  implicit val TimestampGetter: Getter[Timestamp] = {
    (row: ConnectedRow, ix: Int) =>
      Option(row.getTimestamp(ix))
  }

}

trait DateGetter {
  self: DBMS =>

  implicit val DateGetter: Getter[Date] = {
    (row: ConnectedRow, ix: Int) =>
      Option(row.getDate(ix))
  }

}

trait TimeGetter {
  self: DBMS =>

  implicit val TimeGetter: Getter[Time] = {
    (row: ConnectedRow, ix: Int) =>
      Option(row.getTime(ix))
  }

}

trait LocalDateTimeGetter {
  self: DBMS =>

  implicit val LocalDateTimeGetter: Getter[LocalDateTime] =
    (row: ConnectedRow, ix: Int) =>
      Option(row.getTimestamp(ix)).map(_.toLocalDateTime)
}

trait InstantGetter {
  self: DBMS =>

  implicit val InstantGetter: Getter[Instant] =
    (row: ConnectedRow, ix: Int) =>
      Option(row.getTimestamp(ix)).map(_.toInstant)
}

trait LocalDateGetter {
  self: DBMS =>

  implicit val LocalDateGetter: Getter[LocalDate] =
    (row: ConnectedRow, ix: Int) =>
      Option(row.getDate(ix)).map(_.toLocalDate)

}

trait LocalTimeGetter {
  self: DBMS =>

  implicit val LocalTimeGetter: Getter[LocalTime] =
    (row: ConnectedRow, ix: Int) =>
      Option(row.getTime(ix)).map(_.toLocalTime)
}

trait BooleanGetter {
  self: DBMS =>

  implicit val BooleanGetter: Getter[Boolean] =
    ofVal[Boolean] { (row, ix) => row.getBoolean(ix) }

  implicit val BoxedBooleanGetter: Getter[lang.Boolean] =
    (row: ConnectedRow, ix: Int) =>
      BooleanGetter(row, ix).map(lang.Boolean.valueOf)
}

trait StringGetter {
  self: DBMS =>

  implicit val StringGetter: Getter[String] =
    (row: ConnectedRow, ix: Int) =>
      Option(row.getString(ix))

}

trait UUIDGetter {
  self: DBMS =>

  implicit val UUIDGetter: Getter[UUID] =
    (row: ConnectedRow, ix: Int) =>
      Option(row.getObject(ix)).map {
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
    (row: ConnectedRow, ix: Int) =>
      Option(row.getURL(ix))

}
