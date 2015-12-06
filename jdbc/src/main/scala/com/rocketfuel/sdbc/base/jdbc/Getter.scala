package com.rocketfuel.sdbc.base.jdbc

import java.io.{InputStream, Reader}
import java.lang
import java.net.URL
import java.nio.ByteBuffer
import java.sql.{Date, SQLException, Time, Timestamp}
import java.time._
import java.util.UUID

import com.rocketfuel.sdbc.base
import scodec.bits.ByteVector

trait Getter extends CompositeGetter {
  self: Row
    with MutableRow
    with Index
    with ResultSetImplicits =>

  /**
    * This is a type that has a companion object with DBMS-specific implicit getters.
    * @tparam T
    */
  case class Getter[+T](getter: base.Getter[Row, Index.Index, T])
    extends base.Getter[Row, Index.Index, T] {

    override def apply(row: Row, index: Index.Index): T = {
      getter(row, index)
    }

  }

  implicit def baseGetterToGetter[T](getter: base.Getter[Row, Index.Index, T]): Getter[T] = {
    Getter[T](getter)
  }

  def valGetterToGetter[T <: AnyVal](valGetter: (Row, Int) => T): Getter[Option[T]] = {
    (row: Row, ix: Index.Index) =>
      val value = valGetter(row, ix(row))
      if (row.wasNull) None
      else Some(value)
  }

  implicit def compositeGetterToGetter[T](implicit compositeGetter: CompositeGetter[T]): Getter[T] = {
    (v1: Row, v2: Index.Index) =>
      compositeGetter(v1, v2)
  }

  trait Parser[+T] extends base.Getter[Row, Index.Index, Option[T]] {

    override def apply(row: Row, index: Index.Index): Option[T] = {
      Option(row.getString(index(row))).map(parse))
    }

    def parse(s: String): T
  }

}

trait LongGetter {
  self: Getter with Row with Index =>

  implicit val LongGetter: Getter[Option[Long]] =
    valGetterToGetter[Long]((row, ix) => row.getLong(ix))

  implicit val BoxedLongGetter: Getter[Option[lang.Long]] = {
    (row: Row, ix: Index.Index) => LongGetter(row, ix).map(lang.Long.valueOf)
  }

}

trait IntGetter {
  self: Getter with Row with Index =>

  implicit val IntGetter: Getter[Option[Int]] =
    valGetterToGetter[Int]((row, ix) => row.getInt(ix))

  implicit val BoxedIntegerGetter: Getter[Option[lang.Integer]] =
    (row: Row, ix: Index.Index) => IntGetter(row, ix).map(lang.Integer.valueOf)

}

trait ShortGetter {
  self: Getter with Row with Index =>

  implicit val ShortGetter: Getter[Option[Short]] =
    valGetterToGetter[Short] { (row, ix) => row.getShort(ix) }

  implicit val BoxedShortGetter: Getter[Option[lang.Short]] = {
    (row: Row, ix: Index.Index) =>
      ShortGetter(row, ix).map(lang.Short.valueOf)
  }

}

trait ByteGetter {
  self: Getter with Row with Index =>

  implicit val ByteGetter: Getter[Option[Byte]] =
    valGetterToGetter[Byte] { (row, ix) => row.getByte(ix) }

  implicit val BoxedByteGetter: Getter[Option[lang.Byte]] = {
    (row: Row, ix: Index.Index) =>
      ByteGetter(row, ix).map(lang.Byte.valueOf)
  }

}

trait BytesGetter {
  self: Getter with Row with Index =>

  implicit val ArrayByteGetter: Getter[Option[Array[Byte]]] = {
    (row: Row, ix: Index.Index) =>
      Option(row.getBytes(ix(row)))
  }

  implicit val ByteBufferGetter: Getter[Option[ByteBuffer]] = {
    (row: Row, ix: Index.Index) =>
      ArrayByteGetter(row, ix).map(ByteBuffer.wrap)
  }

  implicit val ByteVectorGetter: Getter[Option[ByteVector]] = {
    (row: Row, ix: Index.Index) =>
      ArrayByteGetter(row, ix).map(ByteVector.apply)
  }

}

trait SeqGetter {
  self: BytesGetter
    with Getter
    with Row
    with Index
    with ResultSetImplicits =>

  implicit def toSeqGetter[T](implicit getter: Getter[T]): Getter[Option[Seq[T]]] = {
    (row: Row, ix: Index.Index) =>
      for {
        a <- Option(row.getArray(ix(row)))
      } yield {
        val arrayIterator = a.getResultSet().iterator()
        val arrayValues = for {
          arrayRow <- arrayIterator
        } yield {
          arrayRow.get[T](1)
        }
        arrayValues.toVector
      }
  }

  //Override what would be the inferred Seq[Byte] getter, because you can't use ResultSet#getArray
  //to get the bytes.
  implicit val SeqByteGetter: Getter[Option[Seq[Byte]]] = {
    (row: Row, ix: Index.Index) =>
      ArrayByteGetter(row, ix).map(_.toSeq)
  }

}

trait FloatGetter {
  self: Getter with Row with Index =>

  implicit val FloatGetter: Getter[Option[Float]] =
    valGetterToGetter[Float] { (row, ix) => row.getFloat(ix) }

  implicit val BoxedFloatGetter: Getter[Option[lang.Float]] = {
    (row: Row, ix: Index.Index) =>
      FloatGetter(row, ix).map(lang.Float.valueOf)
  }

}

trait DoubleGetter {
  self: Getter with Row with Index =>

  implicit val DoubleGetter: Getter[Option[Double]] =
    valGetterToGetter[Double]((row, ix) => row.getDouble(ix))

  implicit val BoxedDoubleGetter: Getter[Option[lang.Double]] =
    (row: Row, ix: Index.Index) =>
      DoubleGetter(row, ix).map(lang.Double.valueOf)
}

trait JavaBigDecimalGetter {
  self: Getter with Row with Index =>

  implicit val JavaBigDecimalGetter: Getter[Option[java.math.BigDecimal]] =
    (row: Row, ix: Index.Index) => Option(row.getBigDecimal(ix(row)))

}

trait ScalaBigDecimalGetter {
  self: Getter with Row with Index =>

  implicit val ScalaBigDecimalGetter: Getter[Option[BigDecimal]] = {
    (row: Row, ix: Index.Index) =>
      Option(row.getBigDecimal(ix(row))).map(x => x)
  }

}

trait TimestampGetter {
  self: Getter with Row with Index =>

  implicit val TimestampGetter: Getter[Option[Timestamp]] = {
    (row: Row, ix: Index.Index) =>
      Option(row.getTimestamp(ix(row)))
  }

}

trait DateGetter {
  self: Getter with Row with Index =>

  implicit val DateGetter: Getter[Option[Date]] = {
    (row: Row, ix: Index.Index) =>
      Option(row.getDate(ix(row)))
  }

}

trait TimeGetter {
  self: Getter with Row with Index =>

  implicit val TimeGetter: Getter[Option[Time]] = {
    (row: Row, ix: Index.Index) =>
      Option(row.getTime(ix(row)))
  }

}

trait LocalDateTimeGetter {
  self: Getter with Row with Index =>

  implicit val LocalDateTimeGetter: Getter[Option[LocalDateTime]] =
    (row: Row, ix: Index.Index) =>
      Option(row.getTimestamp(ix(row))).map(_.toLocalDateTime)
}

trait InstantGetter {
  self: Getter with Row with Index =>

  implicit val InstantGetter: Getter[Option[Instant]] =
    (row: Row, ix: Index.Index) =>
      Option(row.getTimestamp(ix(row))).map(_.toInstant)
}

trait LocalDateGetter {
  self: Getter with Row with Index =>

  implicit val LocalDateGetter: Getter[Option[LocalDate]] =
    (row: Row, ix: Index.Index) =>
      Option(row.getDate(ix(row))).map(_.toLocalDate)

}

trait LocalTimeGetter {
  self: Getter with Row with Index =>

  implicit val LocalTimeGetter: Getter[Option[LocalTime]] =
    (row: Row, ix: Index.Index) =>
      Option(row.getTime(ix(row))).map(_.toLocalTime)
}

trait BooleanGetter {
  self: Getter with Row with Index =>

  implicit val BooleanGetter: Getter[Option[Boolean]] =
    valGetterToGetter[Boolean] { (row, ix) => row.getBoolean(ix) }

  implicit val BoxedBooleanGetter: Getter[Option[lang.Boolean]] =
    (row: Row, ix: Index.Index) =>
      BooleanGetter(row, ix).map(lang.Boolean.valueOf)
}

trait StringGetter {
  self: Getter =>

  implicit val StringGetter: Getter[Option[String]] =
    new Parser[String] {
      override def parse(asString: String): String = asString
    }

}

trait UUIDGetter {
  self: Getter with Row with Index =>

  implicit val UUIDGetter: Getter[Option[UUID]] =
    (row: Row, ix: Index.Index) =>
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
  self: Getter with Row with Index =>

  implicit val URLGetter: Getter[Option[URL]] =
    (row: Row, ix: Index.Index) =>
      Option(row.getURL(ix(row)))
}

trait InputStreamGetter {
  self: Getter with Row with Index =>

  implicit val InputStreamGetter: Getter[Option[InputStream]] = {
    (row: Row, ix: Index.Index) =>
      Option(row.getBinaryStream(ix(row)))
  }

}

trait ReaderGetter {
  self: Getter with Row with Index =>

  implicit val ReaderGetter: Getter[Option[Reader]] = {
    (row: Row, ix: Index.Index) =>
      Option(row.getCharacterStream(ix(row)))
  }

}

trait ParameterGetter {
  self: Getter with Row with Index with ParameterValue =>

    implicit val ParameterGetter: Getter[Option[ParameterValue]]

}
