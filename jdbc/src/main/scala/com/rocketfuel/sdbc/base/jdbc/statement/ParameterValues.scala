package com.rocketfuel.sdbc.base.jdbc.statement

import java.io.{InputStream, Reader}
import java.lang
import java.net.URL
import java.nio.ByteBuffer
import java.sql.{Array => _, Date => JdbcDate, _}
import java.time._
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.xml.{Node, NodeSeq}
import scodec.bits.ByteVector

trait LongParameter {
  self: ParameterValue =>

  implicit val LongParameter = new Parameter[Long] {
    override val set: Long => (PreparedStatement, Int) => PreparedStatement = {
      (value) => (statement, parameterIndex) =>
        statement.setLong(parameterIndex + 1, value)
        statement
    }
  }

  implicit val BoxedLongParameter = DerivedParameter[lang.Long, Long]

}

trait IntParameter {
  self: ParameterValue =>

  implicit val IntParameter = new Parameter[Int] {
    override val set: Int => (PreparedStatement, Int) => PreparedStatement = {
      (value) => (statement, parameterIndex) =>
        statement.setInt(parameterIndex + 1, value)
        statement
    }
  }

  implicit val BoxedIntParameter = DerivedParameter[Integer, Int]

}

trait ShortParameter {
  self: ParameterValue =>

  implicit val ShortParameter = new Parameter[Short] {
    override val set: Short => (PreparedStatement, Int) => PreparedStatement = {
      (value) => (statement, parameterIndex) =>
        statement.setShort(parameterIndex + 1, value)
        statement
    }
  }

  implicit val BoxedShortParameter = DerivedParameter[lang.Short, Short]

}

trait ByteParameter {
  self: ParameterValue =>

  implicit val ByteParameter = new Parameter[Byte] {
    override val set: Byte => (PreparedStatement, Int) => PreparedStatement = {
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
    (value: ByteVector) => (statement: PreparedStatement, parameterIndex: Int) =>
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

  implicit val FloatParameter = new Parameter[Float] {
    override val set: Float => (PreparedStatement, Int) => PreparedStatement = {
      (value) => (statement, parameterIndex) =>
        statement.setFloat(parameterIndex + 1, value)
        statement
    }
  }

  implicit val BoxedFloatParameter = DerivedParameter[lang.Float, Float]

}

trait DoubleParameter {
  self: ParameterValue =>

  implicit val DoubleParameter = new Parameter[Double] {
    override val set: Double => (PreparedStatement, Int) => PreparedStatement = {
      (value) => (statement, parameterIndex) =>
        statement.setDouble(parameterIndex + 1, value)
        statement
    }
  }

  implicit val BoxedDoubleParameter = DerivedParameter[lang.Double, Double]

}

trait BigDecimalParameter {
  self: ParameterValue =>

  implicit val JavaBigDecimalParameter = new Parameter[java.math.BigDecimal] {
    override val set: java.math.BigDecimal => (PreparedStatement, Int) => PreparedStatement = {
      (value) => (statement, parameterIndex) =>
        statement.setBigDecimal(parameterIndex + 1, value)
        statement
    }
  }

  implicit val BigDecimalParameter = new DerivedParameter[BigDecimal] {
    override type B = java.math.BigDecimal
    override val conversion: BigDecimal => B = _.underlying()
    override val baseParameter: Parameter[java.math.BigDecimal] = JavaBigDecimalParameter
  }

}

trait TimestampParameter {
  self: ParameterValue =>

  implicit val TimestampParameter = new Parameter[Timestamp] {
    override val set: Timestamp => (PreparedStatement, Int) => PreparedStatement = {
      (value) => (statement, parameterIndex) =>
        statement.setTimestamp(parameterIndex + 1, value)
        statement
    }
  }

  implicit val InstantParameter: Parameter[Instant] =
    new DerivedParameter[Instant] {
      override type B = Timestamp
      override val conversion: Instant => B = Timestamp.from
      override val baseParameter: Parameter[B] = TimestampParameter
    }

  implicit val LocalDateTimeParameter: Parameter[LocalDateTime] =
    new DerivedParameter[LocalDateTime] {
      override type B = Timestamp
      override val conversion: LocalDateTime => B = Timestamp.valueOf
      override val baseParameter: Parameter[B] = TimestampParameter
    }

}

trait DateParameter {
  self: ParameterValue =>

  implicit val JdbcDateParameter = new Parameter[JdbcDate] {
    override val set: JdbcDate => (PreparedStatement, Int) => PreparedStatement = {
      (value) => (statement, parameterIndex) =>
        statement.setDate(parameterIndex + 1, value)
        statement
    }
  }

  implicit val LocalDateParameter: Parameter[LocalDate] =
    new DerivedParameter[LocalDate] {
      override type B = JdbcDate
      override val conversion: LocalDate => B = JdbcDate.valueOf
      override val baseParameter: Parameter[B] = JdbcDateParameter
    }

}

trait TimeParameter {
  self: ParameterValue =>

  implicit val TimeParameter = new Parameter[Time] {
    override val set: Time => (PreparedStatement, Int) => PreparedStatement = {
      (value) => (statement, parameterIndex) =>
        statement.setTime(parameterIndex + 1, value)
        statement
    }
  }

  implicit val LocalTimeParameter: Parameter[LocalTime] =
    new DerivedParameter[LocalTime] {
      override type B = Time
      override val conversion: LocalTime => B = Time.valueOf
      override val baseParameter: Parameter[B] = TimeParameter
    }

}

trait BooleanParameter {
  self: ParameterValue =>

  implicit val BooleanParameter = new Parameter[Boolean] {
    override val set: Boolean => (PreparedStatement, Int) => PreparedStatement = {
      (value) => (statement, parameterIndex) =>
        statement.setBoolean(parameterIndex + 1, value)
        statement
    }
  }

  implicit val BoxedBooleanParameter = DerivedParameter[lang.Boolean, Boolean]

}

trait StringParameter {
  self: ParameterValue =>

  implicit val StringParameter = new Parameter[String] {
    override val set: String => (PreparedStatement, Int) => PreparedStatement = {
      (value) => (statement, parameterIndex) =>
        statement.setString(parameterIndex + 1, value)
        statement
    }
  }

}

trait ReaderParameter {
  self: ParameterValue =>

  implicit val ReaderParameter = new Parameter[Reader] {
    override val set: (Reader) => (PreparedStatement, Int) => PreparedStatement = {
      (value: Reader) => (statement: PreparedStatement, parameterIndex: Int) =>
        statement.setCharacterStream(parameterIndex + 1, value)
        statement
    }
  }

}

trait InputStreamParameter {
  self: ParameterValue =>

  implicit val InputStreamParameter = new Parameter[InputStream] {
    override val set: (InputStream) => (PreparedStatement, Int) => PreparedStatement = {
      (value: InputStream) => (statement: PreparedStatement, parameterIndex: Int) =>
        statement.setBinaryStream(parameterIndex + 1, value)
        statement
    }
  }

}

trait UUIDParameter {
  self: ParameterValue =>

  implicit val UUIDParameter = new Parameter[UUID] {
    override val set: (UUID) => (PreparedStatement, Int) => PreparedStatement = {
      (value: UUID) => (statement: PreparedStatement, parameterIndex: Int) =>
        statement.setObject(parameterIndex + 1, value)
        statement
    }
  }

}

//This is left out of the defaults, since no one seems to support it.
//jTDS supports it, but SQL Server doesn't have a url type.
trait URLParameter {
  self: ParameterValue =>

  implicit val URLParameter = new Parameter[URL] {
    override val set: (URL) => (PreparedStatement, Int) => PreparedStatement = {
      (value: URL) => (statement: PreparedStatement, parameterIndex: Int) =>
        statement.setURL(parameterIndex + 1, value)
        statement
    }
  }

}

trait SQLXMLParameter {
  self: ParameterValue
    with StringParameter =>

  implicit val SQLXMLParameter = new Parameter[SQLXML] {
    override val set: (SQLXML) => (PreparedStatement, Int) => PreparedStatement = {
      (value: SQLXML) => (statement: PreparedStatement, parameterIndex: Int) =>
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
  implicit val NodeParameter: Parameter[NodeSeq] =
    new DerivedParameter[NodeSeq] {
      override type B = String
      override val conversion: NodeSeq => B = nodes => NodeSeq.fromSeq(nodes).toString
      override val baseParameter: Parameter[B] = StringParameter
    }

}

trait BlobParameter {
  self: ParameterValue =>

  implicit val BlobParameter = new Parameter[Blob] {
    override val set: (Blob) => (PreparedStatement, Int) => PreparedStatement = {
      (value: Blob) => (statement: PreparedStatement, parameterIndex: Int) =>
        statement.setBlob(parameterIndex + 1, value)
        statement
    }
  }

}

