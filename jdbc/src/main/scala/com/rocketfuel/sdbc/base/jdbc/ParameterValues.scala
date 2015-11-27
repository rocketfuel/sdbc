package com.rocketfuel.sdbc.base.jdbc

import java.io.{InputStream, Reader}
import java.net.URL
import java.nio.ByteBuffer
import java.sql.{Array => JdbcArray, _}
import java.util.UUID
import com.rocketfuel.sdbc.base.{CompositeParameter, ToParameter}
import scodec.bits.ByteVector
import scala.xml.Node

object QLong extends ToParameter with QLongImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case l: Long => l
    case l: java.lang.Long => l.longValue()
  }
}

trait QLongImplicits {
  implicit val LongIsParameter: IsParameter[Long] = new IsParameter[Long] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Long): Unit = {
      preparedStatement.setLong(
        parameterIndex,
        parameter
      )
    }
  }

  implicit def LongToParameterValue(x: Long): ParameterValue = ParameterValue(x)

  implicit def BoxedLongToParameterValue(x: java.lang.Long): ParameterValue = Long.unbox(x)
}

object QInt extends ToParameter with QIntImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case i: Int => i
    case i: java.lang.Integer => i.value
  }
}

trait QIntImplicits {
  implicit val IntIsParameter: IsParameter[Int] = new IsParameter[Int] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Int): Unit = {
      preparedStatement.setInt(
        parameterIndex,
        parameter
      )
    }
  }

  implicit def IntToParameterValue(x: Int): ParameterValue = ParameterValue(x)

  implicit def BoxedIntToParameterValue(x: java.lang.Integer): ParameterValue = Int.unbox(x)
}

object QShort extends ToParameter with QShortImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case s: Short => s
    case s: java.lang.Short => s.value
  }
}

trait QShortImplicits {
  implicit val ShortIsParameter: IsParameter[Short] = new IsParameter[Short] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Short): Unit = {
      preparedStatement.setShort(
        parameterIndex,
        parameter
      )
    }
  }

  implicit def ShortToParameterValue(x: Short): ParameterValue = ParameterValue(x)

  implicit def BoxedShortToParameterValue(x: java.lang.Short): ParameterValue = Short.unbox(x)
}


object QByte extends ToParameter with QByteImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case b: Byte => b
    case b: java.lang.Byte => b.value
  }
}

trait QByteImplicits {
  implicit val ByteIsParameter: IsParameter[Byte] = new IsParameter[Byte] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Byte): Unit = {
      preparedStatement.setByte(
        parameterIndex,
        parameter
      )
    }
  }

  implicit def ByteToParameterValue(x: Byte): ParameterValue = ParameterValue(x)

  implicit def BoxedByteToParameterValue(x: java.lang.Byte): ParameterValue = Byte.unbox(x)
}

object QBytes extends ToParameter with QBytesImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case b: Array[Byte] => b.value
    case b: ByteBuffer => b.value
    case b: ByteVector => b
  }
}

trait QBytesImplicits {
  //We're using ByteVectors, since they're much more easily testable than Array[Byte].
  //IE equality actually works.
  implicit val ByteVectorIsParameter: IsParameter[ByteVector] = new IsParameter[ByteVector] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: ByteVector): Unit = {
      preparedStatement.setBytes(
        parameterIndex,
        parameter.toArray
      )
    }
  }

  implicit def ArrayByteToParameterValue(x: Array[Byte]): ParameterValue = ParameterValue(ByteVector(x))

  implicit def ByteBufferToParameterValue(x: ByteBuffer): ParameterValue = ParameterValue(ByteVector(x))

  implicit def ByteVectorToParameterValue(x: ByteVector): ParameterValue = ParameterValue(x)
}

object QFloat extends ToParameter with QFloatImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case f: Float => f
    case f: java.lang.Float => f.value
  }
}

trait QFloatImplicits {
  implicit val FloatIsParameter: IsParameter[Float] = new IsParameter[Float] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Float): Unit = {
      preparedStatement.setFloat(
        parameterIndex,
        parameter
      )
    }
  }

  implicit def FloatToParameterValue(x: Float): ParameterValue = ParameterValue(x)

  implicit def BoxedFloatToParameterValue(x: java.lang.Float): ParameterValue = Float.unbox(x)
}

object QDouble extends ToParameter with QDoubleImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case d: Double => d
    case d: java.lang.Double => d.value
  }
}

trait QDoubleImplicits {
  implicit val DoubleIsParameter: IsParameter[Double] = new IsParameter[Double] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Double): Unit = {
      preparedStatement.setDouble(
        parameterIndex,
        parameter
      )
    }
  }

  implicit def DoubleToParameterValue(x: Double): ParameterValue = ParameterValue(x)

  implicit def BoxedDoubleToParameterValue(x: java.lang.Double): ParameterValue = Double.unbox(x)
}

object QBigDecimal extends ToParameter with QBigDecimalImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case d: BigDecimal => d.value
    case d: java.math.BigDecimal => d
  }
}

trait QBigDecimalImplicits {
  implicit val BigDecimalIsParameter: IsParameter[java.math.BigDecimal] = new IsParameter[java.math.BigDecimal] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: java.math.BigDecimal): Unit = {
      preparedStatement.setBigDecimal(
        parameterIndex,
        parameter
      )
    }
  }

  implicit def JavaBigDecimalToParameterValue(x: java.math.BigDecimal): ParameterValue = ParameterValue(x)

  implicit def ScalaBigDecimalToParameterValue(x: scala.BigDecimal): ParameterValue = x.underlying
}

object QTimestamp extends ToParameter with QTimestampImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case t: Timestamp => t
  }
}

trait QTimestampImplicits {
  implicit val TimestampIsParameter: IsParameter[Timestamp] = new IsParameter[Timestamp] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Timestamp): Unit = {
      preparedStatement.setTimestamp(parameterIndex, parameter)
    }
  }

  implicit def TimestampToParameterValue(x: Timestamp): ParameterValue = ParameterValue(x)
}

object QDate extends ToParameter with QDateImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case d: Date => d
    case d: java.util.Date => d.value
  }
}

trait QDateImplicits {
  implicit val DateIsParameter: IsParameter[Date] = new IsParameter[Date] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Date): Unit = {
      preparedStatement.setDate(parameterIndex, parameter)
    }
  }

  implicit def DateToParameterValue(x: Date): ParameterValue = ParameterValue(x)

  implicit def JavaDateToParameterValue(x: java.util.Date): ParameterValue = ParameterValue(new Date(x.getTime))
}

object QTime extends ToParameter with QTimeImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case t: Time => t
  }
}

trait QTimeImplicits {
  implicit val TimeIsParameter: IsParameter[Time] = new IsParameter[Time] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Time): Unit = {
      preparedStatement.setTime(parameterIndex, parameter)
    }
  }

  implicit def TimeToParameterValue(x: Time): ParameterValue = ParameterValue(x)
}

object QBoolean extends ToParameter with QBooleanImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case b: Boolean => b
    case b: java.lang.Boolean => b.value
  }

}

trait QBooleanImplicits {
  implicit val BooleanIsParameter: IsParameter[Boolean] = new IsParameter[Boolean] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Boolean): Unit = {
      preparedStatement.setBoolean(parameterIndex, parameter)
    }
  }

  implicit def BooleanToParameterValue(x: Boolean): ParameterValue = ParameterValue(x)

  implicit def BoxedBooleanToParameterValue(x: java.lang.Boolean): ParameterValue = Boolean.unbox(x)
}

object QString extends ToParameter with QStringImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case s: String => s
  }
}

trait QStringImplicits {
  implicit val StringIsParameter: IsParameter[String] = new IsParameter[String] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: String): Unit = {
      preparedStatement.setString(parameterIndex, parameter)
    }
  }

  implicit def StringToParameterValue(x: String): ParameterValue = ParameterValue(x)
}

object QReader extends ToParameter with QReaderImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case r: Reader => r
  }
}

trait QReaderImplicits {
  implicit val ReaderIsParameter: IsParameter[Reader] = new IsParameter[Reader] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Reader): Unit = {
      preparedStatement.setCharacterStream(parameterIndex, parameter)
    }
  }

  implicit def ReaderToParameterValue(x: Reader): ParameterValue = ParameterValue(x)
}

object QInputStream extends ToParameter with QInputStreamImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case i: InputStream => i
  }
}

trait QInputStreamImplicits {
  implicit val InputStreamIsParameter: IsParameter[InputStream] = new IsParameter[InputStream] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: InputStream): Unit = {
      preparedStatement.setBinaryStream(parameterIndex, parameter)
    }
  }

  implicit def InputStreamToParameterValue(x: InputStream): ParameterValue = ParameterValue(x)
}

object QUUID extends ToParameter with QUUIDImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case u: UUID => u
  }

}
trait QUUIDImplicits {
  implicit val UUIDIsParameter: IsParameter[UUID] = new IsParameter[UUID] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: UUID): Unit = {
      preparedStatement.setObject(parameterIndex, parameter)
    }
  }

  implicit def UUIDToParameterValue(x: UUID): ParameterValue = ParameterValue(x)
}

//This is left out of the defaults, since no one seems to support it.
//jTDS supports it, but SQL Server doesn't have a url type.
object QURL extends ToParameter with QURLImplicits {
  override val toParameter: PartialFunction[Any, ParameterValue] = {
    case u: URL => u
  }
}

trait QURLImplicits {
  implicit val URLIsParameter: IsParameter[URL] = new IsParameter[URL] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: URL): Unit = {
      preparedStatement.setURL(parameterIndex, parameter)
    }
  }

  implicit def URLToParameterValue(u: URL): ParameterValue = {
    ParameterValue(u)
  }
}

trait QArrayImplicits {
  implicit val ArrayIsParameter: IsParameter[JdbcArray] = new IsParameter[JdbcArray] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: JdbcArray): Unit = {
      preparedStatement.setArray(parameterIndex, parameter)
    }
  }

  implicit def JdbcArrayToParameterValue(a: JdbcArray): ParameterValue = {
    ParameterValue(a)
  }
}

object QArray extends ToParameter with QArrayImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case a: JdbcArray => a
  }
}

trait QXMLImplicits {
  implicit val NodeIsParameter: IsParameter[Node] = new IsParameter[Node] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Node): Unit = {
      val sqlxml = preparedStatement.getConnection.createSQLXML()
      sqlxml.setString(parameter.toString)
      preparedStatement.setSQLXML(parameterIndex, sqlxml)
    }
  }

  implicit def NodeToParameterValue(a: Node): ParameterValue = {
    ParameterValue(a)
  }
}

object QXML extends ToParameter with QXMLImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case a: Node => a
  }
}

trait QSQLXMLImplicits {
  implicit val SQLXMLIsParameter: IsParameter[SQLXML] = new IsParameter[SQLXML] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: SQLXML): Unit = {
      preparedStatement.setSQLXML(parameterIndex, parameter)
    }
  }

  implicit def SQLXMLToParameterValue(a: SQLXML): ParameterValue = {
    ParameterValue(a)
  }
}

object QSQLXML extends ToParameter with QSQLXMLImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case a: SQLXML => a
  }
}

trait QBlobImplicits {
  implicit val QNodeIsParameter: IsParameter[Blob] = new IsParameter[Blob] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Blob): Unit = {
      preparedStatement.setBlob(parameterIndex, parameter)
    }
  }

  implicit def BlobToParameterValue(a: Blob): ParameterValue = {
    ParameterValue(a)
  }
}

object QBlob extends ToParameter with QBlobImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case a: Blob => a
  }
}

object QInstant extends ToParameter with QInstantImplicits {

  override val toParameter: PartialFunction[Any, Any] = {
    case i: java.time.Instant => i.value
  }

}

trait QInstantImplicits {
  implicit def InstantToParameterValue(x: java.time.Instant): ParameterValue = {
    ParameterValue(Timestamp.from(x))
  }
}

object QLocalDate extends ToParameter with QLocalDateImplicits {

  override val toParameter: PartialFunction[Any, Any] = {
    case l: java.time.LocalDate => l.value
  }

}

trait QLocalDateImplicits {
  implicit def LocalDateToParameterValue(x: java.time.LocalDate): ParameterValue = {
    ParameterValue(Date.valueOf(x))
  }
}

object QLocalTime extends ToParameter with QLocalTimeImplicits {

  override val toParameter: PartialFunction[Any, Any] = {
    case l: java.time.LocalTime => l.value
  }

}

trait QLocalTimeImplicits {
  implicit def LocalTimeToParameterValue(x: java.time.LocalTime): ParameterValue = {
    ParameterValue(Time.valueOf(x))
  }
}

object QLocalDateTime extends ToParameter with QLocalDateTimeImplicits {

  override val toParameter: PartialFunction[Any, Any] = {
    case l: java.time.LocalDateTime => l.value
  }

}

trait QLocalDateTimeImplicits {
  implicit def LocalDateTimeToParameterValue(x: java.time.LocalDateTime): ParameterValue = {
    ParameterValue(Timestamp.valueOf(x))
  }
}

trait QCompositeParameterImplicits {
  implicit def CompositeValueToParameterValue[A](value: A)(implicit setter: CompositeParameter[A]): ParameterValue = {
    ParameterValue(CompositeParameter(value, setter))
  }
}
