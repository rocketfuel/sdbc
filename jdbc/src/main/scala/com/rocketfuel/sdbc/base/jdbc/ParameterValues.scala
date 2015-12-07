package com.rocketfuel.sdbc.base.jdbc

import java.io.{InputStream, Reader}
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

trait LongSetter {
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
}

object IntToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case i: Int => i
    case i: java.lang.Integer => i.intValue()
  }
}

trait IntSetter {
  self: ParameterValue =>

  implicit val IntIsParameter: IsParameter[Int] = new IsParameter[Int] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Int): Unit = {
      preparedStatement.setInt(
        parameterIndex,
        parameter
      )
    }
  }
}

object ShortToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case s: Short => s
    case s: java.lang.Short => s.shortValue()
  }
}

trait ShortSetter {
  self: ParameterValue =>

  implicit val ShortIsParameter: IsParameter[Short] = new IsParameter[Short] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Short): Unit = {
      preparedStatement.setShort(
        parameterIndex,
        parameter
      )
    }
  }
}


object ByteToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case b: Byte => b
    case b: java.lang.Byte => b.byteValue()
  }
}

trait ByteSetter {
  self: ParameterValue =>

  implicit val ByteIsParameter: IsParameter[Byte] = new IsParameter[Byte] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Byte): Unit = {
      preparedStatement.setByte(
        parameterIndex,
        parameter
      )
    }
  }
}

object BytesToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case b: Array[Byte] => ByteVector(b)
    case b: ByteBuffer => ByteVector(b)
    case b: ByteVector => b
  }
}

trait BytesSetter {
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

  implicit def ArrayByteToParameterValue(x: Array[Byte]): ParameterValue = ParameterValue(ByteVector(x))

  implicit def ByteBufferToParameterValue(x: ByteBuffer): ParameterValue = ParameterValue(ByteVector(x))

  implicit def ByteVectorToParameterValue(x: ByteVector): ParameterValue = ParameterValue(x)
}

object FloatToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case f: Float => f
    case f: java.lang.Float => f.floatValue()
  }
}

trait FloatSetter {
  self: ParameterValue =>

  implicit val FloatIsParameter: IsParameter[Float] = new IsParameter[Float] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Float): Unit = {
      preparedStatement.setFloat(
        parameterIndex,
        parameter
      )
    }
  }
}

object DoubleToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case d: Double => d
    case d: java.lang.Double => d.doubleValue()
  }
}

trait DoubleSetter {
  self: ParameterValue =>

  implicit val DoubleIsParameter: IsParameter[Double] = new IsParameter[Double] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Double): Unit = {
      preparedStatement.setDouble(
        parameterIndex,
        parameter
      )
    }
  }
}

object BigDecimalToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case d: BigDecimal => d.underlying()
    case d: java.math.BigDecimal => d
  }
}

trait BigDecimalSetter {
  self: ParameterValue =>

  implicit val BigDecimalIsParameter: IsParameter[java.math.BigDecimal] = new IsParameter[java.math.BigDecimal] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: java.math.BigDecimal): Unit = {
      preparedStatement.setBigDecimal(
        parameterIndex,
        parameter
      )
    }
  }

  implicit def ScalaBigDecimalToParameterValue(x: scala.BigDecimal): ParameterValue =
    ParameterValue(x.underlying)
}

object TimestampToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case t: Timestamp => t
  }
}

trait TimestampSetter {
  self: ParameterValue =>

  implicit val TimestampIsParameter: IsParameter[Timestamp] = new IsParameter[Timestamp] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Timestamp): Unit = {
      preparedStatement.setTimestamp(parameterIndex, parameter)
    }
  }
}

object DateToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case d: Date => d
    case d: java.util.Date => new java.util.Date(d.getTime)
  }
}

trait DateSetter {
  self: ParameterValue =>

  implicit val DateIsParameter: IsParameter[Date] = new IsParameter[Date] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Date): Unit = {
      preparedStatement.setDate(parameterIndex, parameter)
    }
  }

  implicit def DateToParameterValue(x: Date): ParameterValue = ParameterValue(x)

  implicit def JavaDateToParameterValue(x: java.util.Date): ParameterValue = ParameterValue(new Date(x.getTime))
}

object TimeToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case t: Time => t
  }
}

trait TimeSetter {
  self: ParameterValue =>

  implicit val TimeIsParameter: IsParameter[Time] = new IsParameter[Time] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Time): Unit = {
      preparedStatement.setTime(parameterIndex, parameter)
    }
  }

  implicit def TimeToParameterValue(x: Time): ParameterValue = ParameterValue(x)
}

object BooleanToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case b: Boolean => b
    case b: java.lang.Boolean => b.booleanValue()
  }

}

trait BooleanSetter {
  self: ParameterValue =>

  implicit val BooleanIsParameter: IsParameter[Boolean] = new IsParameter[Boolean] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Boolean): Unit = {
      preparedStatement.setBoolean(parameterIndex, parameter)
    }
  }

  implicit def BooleanToParameterValue(x: Boolean): ParameterValue = ParameterValue(x)

  implicit def BoxedBooleanToParameterValue(x: java.lang.Boolean): ParameterValue = Boolean.unbox(x)
}

object StringToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case s: String => s
  }
}

trait StringSetter {
  self: ParameterValue =>

  implicit val StringIsParameter: IsParameter[String] = new IsParameter[String] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: String): Unit = {
      preparedStatement.setString(parameterIndex, parameter)
    }
  }

  implicit def StringToParameterValue(x: String): ParameterValue = ParameterValue(x)
}

object ReaderToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case r: Reader => r
  }
}

trait ReaderSetter {
  self: ParameterValue =>

  implicit val ReaderIsParameter: IsParameter[Reader] = new IsParameter[Reader] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Reader): Unit = {
      preparedStatement.setCharacterStream(parameterIndex, parameter)
    }
  }

  implicit def ReaderToParameterValue(x: Reader): ParameterValue = ParameterValue(x)
}

object InputStreamToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case i: InputStream => i
  }
}

trait InputStreamSetter {
  self: ParameterValue =>

  implicit val InputStreamIsParameter: IsParameter[InputStream] = new IsParameter[InputStream] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: InputStream): Unit = {
      preparedStatement.setBinaryStream(parameterIndex, parameter)
    }
  }

  implicit def InputStreamToParameterValue(x: InputStream): ParameterValue = ParameterValue(x)
}

object UUIDToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case u: UUID => u
  }

}
trait UUIDSetter {
  self: ParameterValue =>

  implicit val UUIDIsParameter: IsParameter[UUID] = new IsParameter[UUID] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: UUID): Unit = {
      preparedStatement.setObject(parameterIndex, parameter)
    }
  }

  implicit def UUIDToParameterValue(x: UUID): ParameterValue = ParameterValue(x)
}

//This is left out of the defaults, since no one seems to support it.
//jTDS supports it, but SQL Server doesn't have a url type.
object URLToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case u: URL => u
  }
}

trait URLSetter {
  self: ParameterValue =>

  implicit val URLIsParameter: IsParameter[URL] = new IsParameter[URL] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: URL): Unit = {
      preparedStatement.setURL(parameterIndex, parameter)
    }
  }

  implicit def URLToParameterValue(u: URL): ParameterValue = {
    ParameterValue(u)
  }
}

trait ArraySetter {
  self: ParameterValue =>

  implicit val ArrayIsParameter: IsParameter[JdbcArray] = new IsParameter[JdbcArray] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: JdbcArray): Unit = {
      preparedStatement.setArray(parameterIndex, parameter)
    }
  }
}

object ArrayToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case a: JdbcArray => a
  }
}

trait XMLSetter {
  self: ParameterValue =>

  implicit val NodeIsParameter: IsParameter[Node] = new IsParameter[Node] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Node): Unit = {
      val sqlxml = preparedStatement.getConnection.createSQLXML()
      sqlxml.setString(parameter.toString)
      preparedStatement.setSQLXML(parameterIndex, sqlxml)
    }
  }

}

object XMLToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case a: Node => a
  }
}

trait SQLXMLSetter {
  self: ParameterValue =>

  implicit val SQLXMLIsParameter: IsParameter[SQLXML] = new IsParameter[SQLXML] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: SQLXML): Unit = {
      preparedStatement.setSQLXML(parameterIndex, parameter)
    }
  }

}

object SQLXMLToParameter extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case a: SQLXML => a
  }
}

trait BlobSetter {
  self: ParameterValue =>

  implicit val QNodeIsParameter: IsParameter[Blob] = new IsParameter[Blob] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Blob): Unit = {
      preparedStatement.setBlob(parameterIndex, parameter)
    }
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

trait InstantSetter {
  self: ParameterValue =>

  implicit def InstantToParameterValue(x: java.time.Instant): ParameterValue = {
    ParameterValue(Timestamp.from(x))
  }

}

object LocalDateToParameter extends ToParameter {

  override val toParameter: PartialFunction[Any, Any] = {
    case l: java.time.LocalDate => Date.valueOf(l)
  }

}

trait LocalDateSetter {
  self: ParameterValue =>

  implicit def LocalDateToParameterValue(x: java.time.LocalDate): ParameterValue = {
    ParameterValue(Date.valueOf(x))
  }

}

object LocalTimeToParameter extends ToParameter {

  override val toParameter: PartialFunction[Any, Any] = {
    case l: java.time.LocalTime => Time.valueOf(l)
  }

}

trait LocalTimeSetter {
  self: ParameterValue =>

  implicit def LocalTimeToParameterValue(x: java.time.LocalTime): ParameterValue = {
    ParameterValue(Time.valueOf(x))
  }

}

object LocalDateTimeToParameter extends ToParameter {

  override val toParameter: PartialFunction[Any, Any] = {
    case l: java.time.LocalDateTime => Timestamp.valueOf(l)
  }

}

trait LocalDateTimeSetter {
  self: ParameterValue =>

  implicit def LocalDateTimeToParameterValue(x: java.time.LocalDateTime): ParameterValue = {
    ParameterValue(Timestamp.valueOf(x))
  }

}
