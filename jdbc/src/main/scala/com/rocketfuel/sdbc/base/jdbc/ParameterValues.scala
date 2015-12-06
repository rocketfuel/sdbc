package com.rocketfuel.sdbc.base.jdbc

import java.io.{InputStream, Reader}
import java.lang
import java.net.URL
import java.nio.ByteBuffer
import java.sql.{Array => JdbcArray, _}
import java.util.UUID
import com.rocketfuel.sdbc.base.ToParameter
import scodec.bits.ByteVector
import scala.xml.Node

object LongToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case l: Long => l
    case l: java.lang.Long => l.longValue()
  }
}

trait LongIsParameter {
  self: ParameterValue =>

  implicit val LongIsParameter: IsParameter[Long] = new IsParameter[Long] {
    override def set(
      preparedStatement: PreparedStatement,
      parameterIndex: Int,
      parameter: Long
    ): Unit = {
      preparedStatement.setLong(
        parameterIndex,
        parameter
      )
    }
  }

  implicit val BoxedLongIsParameter: IsParameter[java.lang.Long] = new IsParameter[java.lang.Long] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: lang.Long): Unit = {
      LongIsParameter.set(preparedStatement, parameterIndex, parameter)
    }
  }
}

object IntToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case i: Int => i
    case i: java.lang.Integer => i.intValue()
  }
}

trait IntIsParameter {
  self: ParameterValue =>

  implicit val IntIsParameter: IsParameter[Int] = new IsParameter[Int] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Int): Unit = {
      preparedStatement.setInt(
        parameterIndex,
        parameter
      )
    }
  }

  implicit def IntToParameterValue(x: Int): ParameterValue = parameterValue(x)

  implicit def BoxedIntToParameterValue(x: java.lang.Integer): ParameterValue = Int.unbox(x)
}

object ShortToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case s: Short => s
    case s: java.lang.Short => s.shortValue()
  }
}

trait ShortIsParameter {
  self: ParameterValue =>

  implicit val ShortIsParameter: IsParameter[Short] = new IsParameter[Short] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Short): Unit = {
      preparedStatement.setShort(
        parameterIndex,
        parameter
      )
    }
  }

  implicit def ShortToParameterValue(x: Short): ParameterValue = parameterValue(x)

  implicit def BoxedShortToParameterValue(x: java.lang.Short): ParameterValue = Short.unbox(x)
}


object ByteToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case b: Byte => b
    case b: java.lang.Byte => b.byteValue()
  }
}

trait ByteIsParameter {
  self: ParameterValue =>

  implicit val ByteIsParameter: IsParameter[Byte] = new IsParameter[Byte] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Byte): Unit = {
      preparedStatement.setByte(
        parameterIndex,
        parameter
      )
    }
  }

  implicit def ByteToParameterValue(x: Byte): ParameterValue = parameterValue(x)

  implicit def BoxedByteToParameterValue(x: java.lang.Byte): ParameterValue = Byte.unbox(x)
}

object BytesToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case b: Array[Byte] => ByteVector(b)
    case b: ByteBuffer => ByteVector(b)
    case b: ByteVector => b
  }
}

trait BytesIsParameter {
  self: ParameterValue =>

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

  implicit def ArrayByteToParameterValue(x: Array[Byte]): ParameterValue = parameterValue(ByteVector(x))

  implicit def ByteBufferToParameterValue(x: ByteBuffer): ParameterValue = parameterValue(ByteVector(x))

  implicit def ByteVectorToParameterValue(x: ByteVector): ParameterValue = parameterValue(x)
}

object FloatToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case f: Float => f
    case f: java.lang.Float => f.floatValue()
  }
}

trait FloatIsParameter {
  self: ParameterValue =>

  implicit val FloatIsParameter: IsParameter[Float] = new IsParameter[Float] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Float): Unit = {
      preparedStatement.setFloat(
        parameterIndex,
        parameter
      )
    }
  }

  implicit def FloatToParameterValue(x: Float): ParameterValue = parameterValue(x)

  implicit def BoxedFloatToParameterValue(x: java.lang.Float): ParameterValue = Float.unbox(x)
}

object DoubleToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case d: Double => d
    case d: java.lang.Double => d.doubleValue()
  }
}

trait DoubleIsParameter {
  self: ParameterValue =>

  implicit val DoubleIsParameter: IsParameter[Double] = new IsParameter[Double] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Double): Unit = {
      preparedStatement.setDouble(
        parameterIndex,
        parameter
      )
    }
  }

  implicit def DoubleToParameterValue(x: Double): ParameterValue = parameterValue(x)

  implicit def BoxedDoubleToParameterValue(x: java.lang.Double): ParameterValue = Double.unbox(x)
}

object BigDecimalToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case d: BigDecimal => d.underlying()
    case d: java.math.BigDecimal => d
  }
}

trait BigDecimalIsParameter {
  self: ParameterValue =>

  implicit val BigDecimalIsParameter: IsParameter[java.math.BigDecimal] = new IsParameter[java.math.BigDecimal] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: java.math.BigDecimal): Unit = {
      preparedStatement.setBigDecimal(
        parameterIndex,
        parameter
      )
    }
  }

  implicit def JavaBigDecimalToParameterValue(x: java.math.BigDecimal): ParameterValue = parameterValue(x)

  implicit def ScalaBigDecimalToParameterValue(x: scala.BigDecimal): ParameterValue = x.underlying
}

object TimestampToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case t: Timestamp => t
  }
}

trait TimestampIsParameter {
  self: ParameterValue =>

  implicit val TimestampIsParameter: IsParameter[Timestamp] = new IsParameter[Timestamp] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Timestamp): Unit = {
      preparedStatement.setTimestamp(parameterIndex, parameter)
    }
  }

  implicit def TimestampToParameterValue(x: Timestamp): ParameterValue = parameterValue(x)
}

object DateToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case d: Date => d
    case d: java.util.Date => new java.util.Date(d.getTime)
  }
}

trait DateIsParameter {
  self: ParameterValue =>

  implicit val DateIsParameter: IsParameter[Date] = new IsParameter[Date] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Date): Unit = {
      preparedStatement.setDate(parameterIndex, parameter)
    }
  }

  implicit def DateToParameterValue(x: Date): ParameterValue = parameterValue(x)

  implicit def JavaDateToParameterValue(x: java.util.Date): ParameterValue = parameterValue(new Date(x.getTime))
}

object TimeToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case t: Time => t
  }
}

trait TimeIsParameter {
  self: ParameterValue =>

  implicit val TimeIsParameter: IsParameter[Time] = new IsParameter[Time] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Time): Unit = {
      preparedStatement.setTime(parameterIndex, parameter)
    }
  }

  implicit def TimeToParameterValue(x: Time): ParameterValue = parameterValue(x)
}

object BooleanToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case b: Boolean => b
    case b: java.lang.Boolean => b.booleanValue()
  }

}

trait BooleanIsParameter {
  self: ParameterValue =>

  implicit val BooleanIsParameter: IsParameter[Boolean] = new IsParameter[Boolean] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Boolean): Unit = {
      preparedStatement.setBoolean(parameterIndex, parameter)
    }
  }

  implicit def BooleanToParameterValue(x: Boolean): ParameterValue = parameterValue(x)

  implicit def BoxedBooleanToParameterValue(x: java.lang.Boolean): ParameterValue = Boolean.unbox(x)
}

object StringToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case s: String => s
  }
}

trait StringIsParameter {
  self: ParameterValue =>

  implicit val StringIsParameter: IsParameter[String] = new IsParameter[String] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: String): Unit = {
      preparedStatement.setString(parameterIndex, parameter)
    }
  }

  implicit def StringToParameterValue(x: String): ParameterValue = parameterValue(x)
}

object ReaderToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case r: Reader => r
  }
}

trait ReaderIsParameter {
  self: ParameterValue =>

  implicit val ReaderIsParameter: IsParameter[Reader] = new IsParameter[Reader] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Reader): Unit = {
      preparedStatement.setCharacterStream(parameterIndex, parameter)
    }
  }

  implicit def ReaderToParameterValue(x: Reader): ParameterValue = parameterValue(x)
}

object InputStreamToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case i: InputStream => i
  }
}

trait InputStreamIsParameter {
  self: ParameterValue =>

  implicit val InputStreamIsParameter: IsParameter[InputStream] = new IsParameter[InputStream] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: InputStream): Unit = {
      preparedStatement.setBinaryStream(parameterIndex, parameter)
    }
  }

  implicit def InputStreamToParameterValue(x: InputStream): ParameterValue = parameterValue(x)
}

object UUIDToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case u: UUID => u
  }

}
trait UUIDIsParameter {
  self: ParameterValue =>

  implicit val UUIDIsParameter: IsParameter[UUID] = new IsParameter[UUID] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: UUID): Unit = {
      preparedStatement.setObject(parameterIndex, parameter)
    }
  }

  implicit def UUIDToParameterValue(x: UUID): ParameterValue = parameterValue(x)
}

//This is left out of the defaults, since no one seems to support it.
//jTDS supports it, but SQL Server doesn't have a url type.
object URLToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case u: URL => u
  }
}

trait URLIsParameter {
  self: ParameterValue =>

  implicit val URLIsParameter: IsParameter[URL] = new IsParameter[URL] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: URL): Unit = {
      preparedStatement.setURL(parameterIndex, parameter)
    }
  }

  implicit def URLToParameterValue(u: URL): ParameterValue = {
    parameterValue(u)
  }
}

trait ArrayIsParameter {
  self: ParameterValue =>

  implicit val ArrayIsParameter: IsParameter[JdbcArray] = new IsParameter[JdbcArray] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: JdbcArray): Unit = {
      preparedStatement.setArray(parameterIndex, parameter)
    }
  }

  implicit def JdbcArrayToParameterValue(a: JdbcArray): ParameterValue = {
    parameterValue(a)
  }
}

object ArrayToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case a: JdbcArray => a
  }
}

trait XMLIsParameter {
  self: ParameterValue =>

  implicit val NodeIsParameter: IsParameter[Node] = new IsParameter[Node] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Node): Unit = {
      val sqlxml = preparedStatement.getConnection.createSQLXML()
      sqlxml.setString(parameter.toString)
      preparedStatement.setSQLXML(parameterIndex, sqlxml)
    }
  }

  implicit def NodeToParameterValue(a: Node): ParameterValue = {
    parameterValue(a)
  }
}

object XMLToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case a: Node => a
  }
}

trait SQLXMLIsParameter {
  self: ParameterValue =>

  implicit val SQLXMLIsParameter: IsParameter[SQLXML] = new IsParameter[SQLXML] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: SQLXML): Unit = {
      preparedStatement.setSQLXML(parameterIndex, parameter)
    }
  }

  implicit def SQLXMLToParameterValue(a: SQLXML): ParameterValue = {
    parameterValue(a)
  }
}

object SQLXMLToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case a: SQLXML => a
  }
}

trait BlobIsParameter {
  self: ParameterValue =>

  implicit val QNodeIsParameter: IsParameter[Blob] = new IsParameter[Blob] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Blob): Unit = {
      preparedStatement.setBlob(parameterIndex, parameter)
    }
  }

  implicit def BlobToParameterValue(a: Blob): ParameterValue = {
    parameterValue(a)
  }
}

object BlobToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case a: Blob => a
  }
}

object InstantToParameter extends ToParameter {

  override val toParameter: PartialFunction[Any, Any] = {
    case i: java.time.Instant => Timestamp.from(i)
  }

}

trait InstantIsParameter {
  self: ParameterValue =>

  implicit def InstantToParameterValue(x: java.time.Instant): ParameterValue = {
    parameterValue(Timestamp.from(x))
  }
}

object LocalDateToParameter extends ToParameter {

  override val toParameter: PartialFunction[Any, Any] = {
    case l: java.time.LocalDate => Date.valueOf(l)
  }

}

trait LocalDateIsParameter {
  self: ParameterValue =>

  implicit def LocalDateToParameterValue(x: java.time.LocalDate): ParameterValue = {
    parameterValue(Date.valueOf(x))
  }
}

object LocalTimeToParameter extends ToParameter {

  override val toParameter: PartialFunction[Any, Any] = {
    case l: java.time.LocalTime => Time.valueOf(l)
  }

}

trait LocalTimeIsParameter {
  self: ParameterValue =>

  implicit def LocalTimeToParameterValue(x: java.time.LocalTime): ParameterValue = {
    parameterValue(Time.valueOf(x))
  }
}

object LocalDateTimeToParameter extends ToParameter {

  override val toParameter: PartialFunction[Any, Any] = {
    case l: java.time.LocalDateTime => Timestamp.valueOf(l)
  }

}

trait LocalDateTimeIsParameter {
  self: ParameterValue =>

  implicit def LocalDateTimeToParameterValue(x: java.time.LocalDateTime): ParameterValue = {
    parameterValue(Timestamp.valueOf(x))
  }
}
