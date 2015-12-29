package com.rocketfuel.sdbc.base.jdbc

import java.io.{InputStream, Reader}
import java.lang
import java.net.URL
import java.nio.ByteBuffer
import java.sql.{Date => JdbcDate, Array => _, _}
import java.time._
import java.time.format.DateTimeFormatter
import java.util.{Date, UUID}
import scala.xml.Node
import scodec.bits.ByteVector

trait LongParameter {
  self: ParameterValue =>

  implicit object LongParameter
    extends Parameter[Long] {
    override val set: Long => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
      statement.setLong(parameterIndex, value)
      statement
    }
  }

  implicit object BoxedLongParameter
    extends SecondaryParameter[lang.Long, Long]

}

trait IntParameter {
  self: ParameterValue =>

  implicit object IntParameter
    extends Parameter[Int] {
    override val set: Int => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
        statement.setInt(parameterIndex, value)
        statement
    }
  }

  implicit object BoxedIntParameter
    extends SecondaryParameter[Integer, Int]

}

trait ShortParameter {
  self: ParameterValue =>

  implicit object ShortParameter
    extends Parameter[Short] {
    override val set: Short => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
      statement.setShort(parameterIndex, value)
      statement
    }
  }

  implicit object BoxedShortParameter
    extends SecondaryParameter[lang.Short, Short]

}

trait ByteParameter {
  self: ParameterValue =>

  implicit object ByteParameter
    extends Parameter[Byte] {
    override val set: Byte => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
      statement.setByte(parameterIndex, value)
      statement
    }
  }

  implicit object BoxedByteParameter
    extends SecondaryParameter[lang.Byte, Byte]

}

trait BytesParameter {
  self: ParameterValue =>

  //We're using ByteVectors, since they're much more easily testable than Array[Byte].
  //IE equality actually works. Also, they're immutable.
  implicit object ByteVectorParameter extends Parameter[ByteVector] {
    override val set: ByteVector => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
      val arrayValue = value.toArray
      statement.setBytes(parameterIndex, arrayValue)
      statement
    }
  }

  implicit object ByteBufferParameter
    extends SecondaryParameter[ByteBuffer, ByteVector]()(ByteVectorParameter, (b: ByteBuffer) => ByteVector(b))

  implicit object ArrayByteParameter
    extends SecondaryParameter[Array[Byte], ByteVector]()(ByteVectorParameter, (b: Array[Byte]) => ByteVector(b))

}

trait FloatParameter {
  self: ParameterValue =>

  implicit object FloatParameter
    extends Parameter[Float] {
    override val set: Float => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
      statement.setFloat(parameterIndex, value)
      statement
    }
  }

  implicit object BoxedFloatParameter
    extends SecondaryParameter[lang.Float, Float]

}

trait DoubleParameter {
  self: ParameterValue =>

  implicit object DoubleParameter
    extends Parameter[Double] {
    override val set: Double => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
      statement.setDouble(parameterIndex, value)
      statement
    }
  }

  implicit object BoxedDoubleParameter
    extends SecondaryParameter[lang.Double, Double]

}

trait BigDecimalParameter {
  self: ParameterValue =>

  implicit object JavaBigDecimalParameter
    extends Parameter[java.math.BigDecimal] {
    override val set: java.math.BigDecimal => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
      statement.setBigDecimal(parameterIndex, value)
      statement
    }
  }

  implicit object BigDecimalParameter
    extends SecondaryParameter[BigDecimal, java.math.BigDecimal]()(JavaBigDecimalParameter, _.underlying())

}

trait TimestampParameter {
  self: ParameterValue =>

  implicit object TimestampParameter
    extends Parameter[Timestamp] {
    override val set: Timestamp => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
        statement.setTimestamp(parameterIndex, value)
        statement
    }
  }

  implicit object JavaDateParameter
    extends SecondaryParameter[Date, Timestamp]()(TimestampParameter, date => new Timestamp(date.getTime))

  implicit object InstantParameter
    extends SecondaryParameter[Instant, Timestamp]()(TimestampParameter, Timestamp.from)

  implicit object LocalDateTimeParameter
    extends SecondaryParameter[LocalDateTime, Timestamp]()(TimestampParameter, Timestamp.valueOf)

}

trait DateParameter {
  self: ParameterValue =>

  implicit object JdbcDateParameter extends Parameter[JdbcDate] {
    override val set: JdbcDate => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
      statement.setDate(parameterIndex, value)
      statement
    }
  }

  implicit object LocalDateParameter
    extends SecondaryParameter[LocalDate, JdbcDate]()(JdbcDateParameter, JdbcDate.valueOf)

}

trait TimeParameter {
  self: ParameterValue =>

  implicit object TimeParameter extends Parameter[Time] {
    override val set: Time => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
      statement.setTime(parameterIndex, value)
      statement
    }
  }

  implicit object LocalTimeParameter
    extends SecondaryParameter[LocalTime, Time]()(TimeParameter, Time.valueOf)

}

trait BooleanParameter {
  self: ParameterValue =>

  implicit object BooleanParameter
    extends Parameter[Boolean] {
    override val set: Boolean => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
      statement.setBoolean(parameterIndex, value)
      statement
    }
  }

  implicit object BoxedBooleanParameter
    extends SecondaryParameter[lang.Boolean, Boolean]

}

trait StringParameter {
  self: ParameterValue =>

  implicit object StringParameter
    extends Parameter[String] {
    override val set: String => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
        statement.setString(parameterIndex, value)
        statement
    }
  }

}

trait ReaderParameter {
  self: ParameterValue =>

  implicit object ReaderParameter
    extends Parameter[Reader] {
    override val set: (Reader) => (Statement, Int) => Statement = {
      (value: Reader) => (statement: Statement, parameterIndex: Int) =>
        statement.setCharacterStream(parameterIndex, value)
        statement
    }
  }

}

trait InputStreamParameter {
  self: ParameterValue =>

  implicit object InputStreamParameter
    extends Parameter[InputStream] {
    override val set: (InputStream) => (Statement, Int) => Statement = {
      (value: InputStream) => (statement: Statement, parameterIndex: Int) =>
        statement.setBinaryStream(parameterIndex, value)
        statement
    }
  }

}

trait UUIDParameter {
  self: ParameterValue =>

  implicit object UUIDParameter
    extends Parameter[UUID] {
    override val set: (UUID) => (Statement, Int) => Statement = {
      (value: UUID) => (statement: Statement, parameterIndex: Int) =>
        statement.setObject(parameterIndex, value)
        statement
    }
  }

}

//This is left out of the defaults, since no one seems to support it.
//jTDS supports it, but SQL Server doesn't have a url type.
trait URLParameter {
  self: ParameterValue =>

  implicit object URLParameter
    extends Parameter[URL] {
    override val set: (URL) => (Statement, Int) => Statement = {
      (value: URL) => (statement: Statement, parameterIndex: Int) =>
        statement.setURL(parameterIndex, value)
        statement
    }
  }

}

trait SQLXMLParameter {
  self: ParameterValue
    with StringParameter =>

  implicit object SQLXMLParameter
    extends Parameter[SQLXML] {
    override val set: (SQLXML) => (PreparedStatement, Int) => PreparedStatement = {
      (value: SQLXML) => (statement: Statement, parameterIndex: Int) =>
        statement.setSQLXML(parameterIndex, value)
        statement
    }
  }

  /**
    * JDBC has a special SQLXML type, but jTDS doesn't support it. Most drivers seem to
    * have no problems sending XML as strings.
    *
    * The PostgreSQL driver, for example, sends the XML as a string with the Oid set as Oid.XML.
    */
  implicit object NodeParameter
    extends SecondaryParameter[Node, String]()(StringParameter, _.toString)

}

trait BlobParameter {
  self: ParameterValue =>

  implicit object BlobParameter
    extends Parameter[Blob] {
    override val set: (Blob) => (Statement, Int) => Statement = {
      (value: Blob) => (statement: Statement, parameterIndex: Int) =>
        statement.setBlob(parameterIndex, value)
        statement
    }
  }

}

trait OffsetDateTimeAsTimestampParameter {
  self: ParameterValue
    with TimestampParameter =>

  implicit object OffsetDateTimeParameter
    extends Parameter[OffsetDateTime] {

    override val set: (OffsetDateTime) => (Statement, Int) => Statement = {
      (value: OffsetDateTime) =>
        val converted = Timestamp.from(value.toInstant)
        TimestampParameter.set(converted)
    }

  }

}

trait OffsetDateTimeAsStringParameter {
  self: ParameterValue
    with StringParameter =>

  val offsetDateTimeFormatter: DateTimeFormatter

  implicit object OffsetDateTimeParameter
    extends Parameter[OffsetDateTime] {
    override val set: (OffsetDateTime) => (Statement, Int) => Statement = {
      value =>
        val formatted = offsetDateTimeFormatter.format(value)
        (statement, parameterIndex) =>
          StringParameter.set(formatted)(statement, parameterIndex)
    }
  }

}
