package com.rocketfuel.sdbc.base.jdbc

import java.io.{InputStream, Reader}
import java.lang
import java.net.URL
import java.nio.ByteBuffer
import java.sql.{Date => JdbcDate, Array => _, _}
import java.time._
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.xml.{NodeSeq, Node}
import scodec.bits.ByteVector

trait LongParameter {
  self: ParameterValue =>

  implicit object LongParameter
    extends Parameter[Long] {
    override val set: Long => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
        statement.setLong(parameterIndex + 1, value)
        statement
    }
  }

  implicit val BoxedLongParameter = DerivedParameter[lang.Long, Long]

}

trait IntParameter {
  self: ParameterValue =>

  implicit object IntParameter
    extends Parameter[Int] {
    override val set: Int => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
        statement.setInt(parameterIndex + 1, value)
        statement
    }
  }

  implicit val BoxedIntParameter = DerivedParameter[Integer, Int]

}

trait ShortParameter {
  self: ParameterValue =>

  implicit object ShortParameter
    extends Parameter[Short] {
    override val set: Short => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
        statement.setShort(parameterIndex + 1, value)
        statement
    }
  }

  implicit val BoxedShortParameter = DerivedParameter[lang.Short, Short]

}

trait ByteParameter {
  self: ParameterValue =>

  implicit object ByteParameter
    extends Parameter[Byte] {
    override val set: Byte => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
        statement.setByte(parameterIndex + 1, value)
        statement
    }
  }

  implicit val BoxedByteParameter = DerivedParameter[lang.Byte, Byte]

}

trait BytesParameter {
  self: ParameterValue =>

  //We're using ByteVectors, since they're much more easily testable than Array[Byte].
  //IE equality actually works. Also, they're immutable.
  implicit val ByteVectorParameter: Parameter[ByteVector] = {
    (value: ByteVector) => (statement: Statement, parameterIndex: Int) =>
      val arrayValue = value.toArray
      statement.setBytes(parameterIndex + 1, arrayValue)
      statement
  }

  implicit val ByteBufferParameter = DerivedParameter[ByteBuffer, ByteVector](ByteVector.apply, ByteVectorParameter)

  implicit val ArrayByteParameter = DerivedParameter[Array[Byte], ByteVector](ByteVector.apply, ByteVectorParameter)

  implicit val SeqByteParameter = DerivedParameter[Seq[Byte], ByteVector](ByteVector.apply, ByteVectorParameter)

}

trait FloatParameter {
  self: ParameterValue =>

  implicit object FloatParameter
    extends Parameter[Float] {
    override val set: Float => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
        statement.setFloat(parameterIndex + 1, value)
        statement
    }
  }

  implicit val BoxedFloatParameter = DerivedParameter[lang.Float, Float]

}

trait DoubleParameter {
  self: ParameterValue =>

  implicit object DoubleParameter
    extends Parameter[Double] {
    override val set: Double => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
        statement.setDouble(parameterIndex + 1, value)
        statement
    }
  }

  implicit val BoxedDoubleParameter = DerivedParameter[lang.Double, Double]

}

trait BigDecimalParameter {
  self: ParameterValue =>

  implicit object JavaBigDecimalParameter
    extends Parameter[java.math.BigDecimal] {
    override val set: java.math.BigDecimal => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
        statement.setBigDecimal(parameterIndex + 1, value)
        statement
    }
  }

  implicit object BigDecimalParameter extends DerivedParameter[BigDecimal] {
    override type B = java.math.BigDecimal
    override val conversion: BigDecimal => B = _.underlying()
    override val baseParameter: Parameter[java.math.BigDecimal] = JavaBigDecimalParameter
  }

}

trait TimestampParameter {
  self: ParameterValue =>

  implicit object TimestampParameter
    extends Parameter[Timestamp] {
    override val set: Timestamp => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
        statement.setTimestamp(parameterIndex + 1, value)
        statement
    }
  }

  implicit object InstantParameter
    extends DerivedParameter[Instant] {
    override type B = Timestamp
    override val conversion: Instant => B = Timestamp.from
    override val baseParameter: Parameter[B] = TimestampParameter
  }

  implicit object LocalDateTimeParameter
    extends DerivedParameter[LocalDateTime] {
    override type B = Timestamp
    override val conversion: LocalDateTime => B = Timestamp.valueOf
    override val baseParameter: Parameter[B] = TimestampParameter
  }

}

trait DateParameter {
  self: ParameterValue =>

  implicit object JdbcDateParameter extends Parameter[JdbcDate] {
    override val set: JdbcDate => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
        statement.setDate(parameterIndex + 1, value)
        statement
    }
  }

  implicit object LocalDateParameter
    extends DerivedParameter[LocalDate] {
    override type B = JdbcDate
    override val conversion: LocalDate => B = JdbcDate.valueOf
    override val baseParameter: Parameter[B] = JdbcDateParameter
  }

}

trait TimeParameter {
  self: ParameterValue =>

  implicit object TimeParameter extends Parameter[Time] {
    override val set: Time => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
        statement.setTime(parameterIndex + 1, value)
        statement
    }
  }

  implicit object LocalTimeParameter
    extends DerivedParameter[LocalTime] {
    override type B = Time
    override val conversion: LocalTime => B = Time.valueOf
    override val baseParameter: Parameter[B] = TimeParameter
  }

}

trait BooleanParameter {
  self: ParameterValue =>

  implicit object BooleanParameter
    extends Parameter[Boolean] {
    override val set: Boolean => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
        statement.setBoolean(parameterIndex + 1, value)
        statement
    }
  }

  implicit val BoxedBooleanParameter = DerivedParameter[lang.Boolean, Boolean]

}

trait StringParameter {
  self: ParameterValue =>

  implicit object StringParameter
    extends Parameter[String] {
    override val set: String => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
        statement.setString(parameterIndex + 1, value)
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
        statement.setCharacterStream(parameterIndex + 1, value)
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
        statement.setBinaryStream(parameterIndex + 1, value)
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
        statement.setObject(parameterIndex + 1, value)
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
        statement.setURL(parameterIndex + 1, value)
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
        statement.setSQLXML(parameterIndex + 1, value)
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
    extends DerivedParameter[Seq[Node]] {
    override type B = String
    override val conversion: Seq[Node] => B = nodes => NodeSeq.fromSeq(nodes).toString
    override val baseParameter: Parameter[B] = StringParameter
  }

}

trait BlobParameter {
  self: ParameterValue =>

  implicit object BlobParameter
    extends Parameter[Blob] {
    override val set: (Blob) => (Statement, Int) => Statement = {
      (value: Blob) => (statement: Statement, parameterIndex: Int) =>
        statement.setBlob(parameterIndex + 1, value)
        statement
    }
  }

}

trait OffsetDateTimeAsTimestampParameter {
  self: ParameterValue
    with TimestampParameter =>

  implicit object OffsetDateTimeParameter
    extends DerivedParameter[OffsetDateTime] {
      override type B = Timestamp
      override val conversion: OffsetDateTime => B = value => Timestamp.from(value.toInstant)
      override val baseParameter: Parameter[B] = TimestampParameter
  }

}

trait OffsetDateTimeAsStringParameter {
  self: ParameterValue
    with StringParameter =>

  val offsetDateTimeFormatter: DateTimeFormatter

  implicit object OffsetDateTimeFormatter
    extends DerivedParameter[OffsetDateTime] {
      override type B = String
      override val conversion: OffsetDateTime => B = offsetDateTimeFormatter.format
      override val baseParameter: Parameter[B] = StringParameter
  }

}
