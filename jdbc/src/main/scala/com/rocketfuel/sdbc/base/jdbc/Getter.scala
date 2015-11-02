package com.rocketfuel.sdbc.base.jdbc

import java.io.{InputStream, Reader}
import java.lang
import java.net.URL
import java.nio.ByteBuffer
import java.sql.{SQLException, Date, Time, Timestamp}
import java.time._
import java.util.UUID

import scodec.bits.ByteVector

object Getter {

  def fromValGetter[T <: AnyVal](valGetter: Row => Int => T): Getter[T] = {
    new Getter[T] {
      override def apply(row: Row, ix: Index): Option[T] = {
        val value = valGetter(row)(ix(row))
        if (row.wasNull) None
        else Some(value)
      }
    }
  }

}

trait Parser[+T] extends Getter[T] {

  override def apply(row: Row, ix: Index): Option[T] = {
    Option(row.getString(ix(row))).map(parse)
  }

  def parse(asString: String): T

}

trait LongGetter {

  implicit val LongGetter =
    Getter.fromValGetter[Long]{ row => ix => row.getLong(ix) }

  implicit val BoxedLongGetter = new Getter[java.lang.Long] {
    override def apply(row: Row, ix: Index): Option[lang.Long] = {
      LongGetter(row, ix).map(java.lang.Long.valueOf)
    }
  }

}

trait IntGetter {

  implicit val IntGetter =
    Getter.fromValGetter[Int]{ row => ix => row.getInt(ix) }

  implicit val BoxedIntegerGetter = new Getter[java.lang.Integer] {
    override def apply(row: Row, ix: Index): Option[lang.Integer] = {
      IntGetter(row, ix).map(java.lang.Integer.valueOf)
    }
  }

}

trait ShortGetter {

  implicit val ShortGetter =
    Getter.fromValGetter[Short]{ row => ix => row.getShort(ix) }

  implicit val BoxedShortGetter = new Getter[java.lang.Short] {
    override def apply(row: Row, ix: Index): Option[lang.Short] = {
      ShortGetter(row, ix).map(java.lang.Short.valueOf)
    }
  }

}

trait ByteGetter {

  implicit val ByteGetter =
    Getter.fromValGetter[Byte]{ row => ix => row.getByte(ix) }

  implicit val BoxedByteGetter = new Getter[java.lang.Byte] {
    override def apply(row: Row, ix: Index): Option[lang.Byte] = {
      ByteGetter(row, ix).map(java.lang.Byte.valueOf)
    }
  }

}

trait BytesGetter {

  implicit val ArrayByteGetter =
  new Getter[Array[Byte]] {
    override def apply(row: Row, ix: Index): Option[Array[Byte]] = {
      Option(row.getBytes(ix(row)))
    }
  }

  implicit val ByteBufferGetter =
    new Getter[ByteBuffer] {
      override def apply(row: Row, ix: Index): Option[ByteBuffer] = {
        ArrayByteGetter(row, ix).map(ByteBuffer.wrap)
      }
    }

  implicit val ByteVectorGetter =
    new Getter[ByteVector] {
      override def apply(row: Row, ix: Index): Option[ByteVector] = {
        ArrayByteGetter(row, ix).map(ByteVector.apply)
      }
    }

}

trait FloatGetter {

  implicit val FloatGetter =
    Getter.fromValGetter[Float]{ row => ix => row.getFloat(ix) }

  implicit val BoxedFloatGetter = new Getter[java.lang.Float] {
    override def apply(row: Row, ix: Index): Option[lang.Float] = {
      FloatGetter(row, ix).map(java.lang.Float.valueOf)
    }
  }

}

trait DoubleGetter {

  implicit val DoubleGetter =
    Getter.fromValGetter[Double]{ row => ix => row.getDouble(ix) }

  implicit val BoxedDoubleGetter = new Getter[java.lang.Double] {
    override def apply(row: Row, ix: Index): Option[lang.Double] = {
      DoubleGetter(row, ix).map(java.lang.Double.valueOf)
    }
  }

}

trait JavaBigDecimalGetter {

  implicit val JavaBigDecimalGetter = new Getter[java.math.BigDecimal] {
    override def apply(row: Row, ix: Index): Option[java.math.BigDecimal] = {
      Option(row.getBigDecimal(ix(row)))
    }
  }

}

trait ScalaBigDecimalGetter {

  implicit val ScalaBigDecimalGetter = new Getter[BigDecimal] {
    override def apply(row: Row, ix: Index): Option[BigDecimal] = {
      Option(row.getBigDecimal(ix(row))).map(x => x)
    }
  }

}

trait TimestampGetter {

  implicit val TimestampGetter = new Getter[Timestamp] {
    override def apply(row: Row, ix: Index): Option[Timestamp] = {
      Option(row.getTimestamp(ix(row)))
    }
  }

}

trait DateGetter {

  implicit val DateGetter = new Getter[Date] {
    override def apply(row: Row, ix: Index): Option[Date] = {
      Option(row.getDate(ix(row)))
    }
  }

}

trait TimeGetter {

  implicit val TimeGetter = new Getter[Time] {
    override def apply(row: Row, ix: Index): Option[Time] = {
      Option(row.getTime(ix(row)))
    }
  }

}

trait LocalDateTimeGetter {

  implicit val LocalDateTimeGetter = new Getter[LocalDateTime] {
    override def apply(row: Row, ix: Index): Option[LocalDateTime] = {
      Option(row.getTimestamp(ix(row))).map(_.toLocalDateTime)
    }
  }

}

trait InstantGetter {

  implicit val InstantGetter = new Getter[Instant] {
    override def apply(row: Row, ix: Index): Option[Instant] = {
      Option(row.getTimestamp(ix(row))).map(_.toInstant)
    }
  }

}

trait LocalDateGetter {

  implicit val LocalDateGetter = new Getter[LocalDate] {
    override def apply(row: Row, ix: Index): Option[LocalDate] = {
      Option(row.getDate(ix(row))).map(_.toLocalDate)
    }
  }

}

trait LocalTimeGetter {

  implicit val LocalTimeGetter = new Getter[LocalTime] {
    override def apply(row: Row, ix: Index): Option[LocalTime] = {
      Option(row.getTime(ix(row))).map(_.toLocalTime)
    }
  }

}

trait BooleanGetter {

  implicit val BooleanGetter: Getter[Boolean] =
    Getter.fromValGetter[Boolean]{ row => ix => row.getBoolean(ix) }

  implicit val BoxedBooleanGetter = new Getter[java.lang.Boolean] {
    override def apply(row: Row, ix: Index): Option[lang.Boolean] = {
      BooleanGetter(row, ix).map(java.lang.Boolean.valueOf)
    }
  }

}

trait StringGetter {

  implicit val StringGetter = new Parser[String] {
    override def parse(asString: String): String = asString
  }

}

trait UUIDGetter {

  implicit val UUIDGetter = new Getter[UUID] {
    override def apply(row: Row, ix: Index): Option[UUID] = {
      Option(row.getObject(ix(row))).map {
        case u: UUID =>
          u
        case s: String =>
          UUID.fromString(s)
        case otherwise =>
          throw new SQLException("UUID value expected but not found.")
      }
    }
  }

}

//This is left out of the defaults, since no one seems to suppor it.
trait URLGetter {

  implicit val URLGetter = new Getter[URL] {
    override def apply(row: Row, ix: Index): Option[URL] = {
      Option(row.getURL(ix(row)))
    }
  }

}

trait InputStreamGetter {

  implicit val InputStreamGetter = new Getter[InputStream] {
    override def apply(row: Row, ix: Index): Option[InputStream] = {
      Option(row.getBinaryStream(ix(row)))
    }
  }

}

trait ReaderGetter {

  implicit val ReaderGetter = new Getter[Reader] {
    override def apply(row: Row, ix: Index): Option[Reader] = {
      Option(row.getCharacterStream(ix(row)))
    }
  }

}

trait SeqGetter {
  self: BytesGetter =>

  implicit def GetterToSeqOptionGetter[T](implicit getter: Getter[T]): Getter[Seq[Option[T]]] = {
    (row: Row, ix: Index) =>
      for {
        a <- Option(row.getArray(ix(row)))
      } yield {
        val arrayIterator = a.getResultSet().iterator()
        val arrayValues = for {
          arrayRow <- arrayIterator
        } yield {
            arrayRow.get[T](IntIndex(1))
          }
        arrayValues.toVector
      }
  }

  implicit def GetterToSeqGetter[T](implicit getter: Getter[T]): Getter[Seq[T]] = {
    (row: Row, ix: Index) =>
      GetterToSeqOptionGetter(getter)(row, ix).map(_.map(_.get))
  }

  //Override what would be the inferred Seq[Byte] getter, because you can't use ResultSet#getArray
  //to get the bytes.
  implicit val SeqByteGetter = new Getter[Seq[Byte]] {
    override def apply(row: Row, ix: Index): Option[Seq[Byte]] = {
      ArrayByteGetter(row, ix).map(_.toSeq)
    }
  }

}

trait ParameterGetter {
  implicit val ParameterGetter: Getter[ParameterValue]
}

trait AnyRefGetter {

  val AnyRefGetter = new Getter[AnyRef] {
    override def apply(row: Row, ix: Index): Option[AnyRef] = {
      Option(row.getObject(ix(row)))
    }
  }

}
