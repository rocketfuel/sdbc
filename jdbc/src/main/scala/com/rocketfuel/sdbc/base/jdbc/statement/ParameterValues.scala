package com.rocketfuel.sdbc.base.jdbc.statement

import java.io.{InputStream, Reader}
import java.lang
import java.net.URL
import java.nio.ByteBuffer
import java.sql.{Date => JdbcDate, Array => _, _}
import java.time._
import java.util.{Date, UUID}
import scala.xml._
import scodec.bits.ByteVector

trait LongParameter {
  self: ParameterValue =>

  implicit val LongParameter: Parameter[Long] = {
    (value: Long) => (statement: PreparedStatement, parameterIndex: Int) =>
      statement.setLong(parameterIndex + 1, value)
      statement
  }

  implicit val BoxedLongParameter: Parameter[lang.Long] =
    Parameter.derived[lang.Long, Long]

}

trait IntParameter {
  self: ParameterValue =>

  implicit val IntParameter: Parameter[Int] = {
    (value: Int) => (statement: PreparedStatement, parameterIndex: Int) =>
    statement.setInt(parameterIndex + 1, value)
    statement
  }

  implicit val BoxedIntParameter: Parameter[lang.Integer] =
    Parameter.derived[Integer, Int]

}

trait ShortParameter {
  self: ParameterValue =>

  implicit val ShortParameter: Parameter[Short] = {
    (value: Short, statement: PreparedStatement, parameterIndex: Int) =>
      statement.setShort(parameterIndex + 1, value)
      statement
  }

  implicit val BoxedShortParameter: Parameter[lang.Short] =
    Parameter.derived[lang.Short, Short]

}

trait ByteParameter {
  self: ParameterValue =>

  implicit val ByteParameter: Parameter[Byte] = {
    (value: Byte, statement: PreparedStatement, parameterIndex: Int) =>
        statement.setByte(parameterIndex + 1, value)
        statement
    }

  implicit val BoxedByteParameter: Parameter[lang.Byte] =
    Parameter.derived[lang.Byte, Byte]

}

trait BytesParameter {
  self: ParameterValue =>

  //We're using ByteVectors, since they're much more easily testable than Array[Byte].
  //IE equality actually works. Also, they're immutable.
  implicit val ByteVectorParameter: Parameter[ByteVector] = {
    (value: ByteVector) => {
      val arrayValue = value.toArray
      (statement: PreparedStatement, parameterIndex: Int) =>
        statement.setBytes(parameterIndex + 1, arrayValue)
        statement
    }
  }

  implicit val ByteBufferParameter: Parameter[ByteBuffer] =
    (bytes: ByteBuffer) => ByteVector(bytes)

  implicit val ArrayByteParameter: Parameter[Array[Byte]] =
    (bytes: Array[Byte]) => ByteVector(bytes)

  implicit val SeqByteParameter: Parameter[Seq[Byte]] =
    (bytes: Seq[Byte]) => ByteVector(bytes)

}

trait FloatParameter {
  self: ParameterValue =>

  implicit val FloatParameter: Parameter[Float] = {
    (value: Float, statement: PreparedStatement, parameterIndex: Int) =>
      statement.setFloat(parameterIndex + 1, value)
      statement
  }

  implicit val BoxedFloatParameter: Parameter[lang.Float] =
    Parameter.derived[lang.Float, Float]

}

trait DoubleParameter {
  self: ParameterValue =>

  implicit val DoubleParameter: Parameter[Double] = {
    (value: Double, statement: PreparedStatement, parameterIndex: Int) =>
      statement.setDouble(parameterIndex + 1, value)
      statement
  }

  implicit val BoxedDoubleParameter: Parameter[lang.Double] =
    Parameter.derived[lang.Double, Double]

}

trait BigDecimalParameter {
  self: ParameterValue =>

  implicit val JavaBigDecimalParameter: Parameter[java.math.BigDecimal] = {
    (value: java.math.BigDecimal, statement: PreparedStatement, parameterIndex: Int) =>
      statement.setBigDecimal(parameterIndex + 1, value)
      statement
  }

  implicit val BigDecimalParameter: Parameter[BigDecimal] =
    _.underlying()

}

trait DateParameter {
  self: ParameterValue =>

  implicit val DateParameter: Parameter[Date] = {
    (_: Date) match {
        case jdbcDate: JdbcDate =>
          (statement: PreparedStatement, parameterIndex: Int) => {
            statement.setDate(parameterIndex + 1, jdbcDate)
            statement
          }
        case jdbcTime: Time =>
          (statement: PreparedStatement, parameterIndex: Int) => {
            statement.setTime(parameterIndex + 1, jdbcTime)
            statement
          }
        case timestamp: Timestamp =>
          (statement: PreparedStatement, parameterIndex: Int) => {
            statement.setTimestamp(parameterIndex + 1, timestamp)
            statement
          }
        case otherwise =>
          (statement: PreparedStatement, parameterIndex: Int) => {
            statement.setDate(parameterIndex + 1, new JdbcDate(otherwise.getTime))
            statement
          }
      }
  }

  implicit val LocalDateParameter: Parameter[LocalDate] =
    (d: LocalDate) => JdbcDate.valueOf(d)

  implicit val LocalTimeParameter: Parameter[LocalTime] =
    (l: LocalTime) => Time.valueOf(l)

  implicit val InstantParameter: Parameter[Instant] =
    (i: Instant) => Timestamp.from(i)

  implicit val LocalDateTimeParameter: Parameter[LocalDateTime] =
    (l: LocalDateTime) => Timestamp.valueOf(l)

}

trait BooleanParameter {
  self: ParameterValue =>

  implicit val BooleanParameter: Parameter[Boolean] = {
    (value: Boolean, statement: PreparedStatement, parameterIndex: Int) =>
      statement.setBoolean(parameterIndex + 1, value)
      statement
  }

  implicit val BoxedBooleanParameter: Parameter[lang.Boolean] =
    Parameter.derived[lang.Boolean, Boolean]

}

trait StringParameter {
  self: ParameterValue =>

  implicit val StringParameter: Parameter[String] = {
    (value: String, statement: PreparedStatement, parameterIndex: Int) =>
      statement.setString(parameterIndex + 1, value)
      statement
  }

}

trait ReaderParameter {
  self: ParameterValue =>

  implicit val ReaderParameter: Parameter[Reader] = {
    (value: Reader, statement: PreparedStatement, parameterIndex: Int) =>
      statement.setCharacterStream(parameterIndex + 1, value)
      statement
  }

}

trait InputStreamParameter {
  self: ParameterValue =>

  implicit val InputStreamParameter: Parameter[InputStream] = {
    (value: InputStream, statement: PreparedStatement, parameterIndex: Int) =>
      statement.setBinaryStream(parameterIndex + 1, value)
      statement
  }

}

trait UUIDParameter {
  self: ParameterValue =>

  implicit val UUIDParameter: Parameter[UUID] = {
    (value: UUID, statement: PreparedStatement, parameterIndex: Int) =>
      statement.setObject(parameterIndex + 1, value)
      statement
  }

}

//This is left out of the defaults, since no one seems to support it.
//jTDS supports it, but SQL Server doesn't have a url type.
trait URLParameter {
  self: ParameterValue =>

  implicit val URLParameter: Parameter[URL] = {
    (value: URL, statement: PreparedStatement, parameterIndex: Int) =>
      statement.setURL(parameterIndex + 1, value)
      statement
  }

}

trait SQLXMLParameter {
  self: ParameterValue
    with StringParameter =>

  /**
    * JDBC has a special SQLXML type, but jTDS doesn't support it. Most drivers seem to
    * have no problems sending XML as strings.
    *
    * The PostgreSQL driver, for example, sends the XML as a string with the Oid set as Oid.XML.
    */
  implicit val XmlElemParameter: Parameter[Elem] =
    Parameter.toString[Elem]

}

trait BlobParameter {
  self: ParameterValue =>

  implicit val BlobParameter: Parameter[Blob] = {
    (value: Blob, statement: PreparedStatement, parameterIndex: Int) =>
      statement.setBlob(parameterIndex + 1, value)
      statement
  }

}
