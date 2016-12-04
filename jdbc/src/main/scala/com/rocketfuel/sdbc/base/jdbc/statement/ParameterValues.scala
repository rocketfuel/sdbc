package com.rocketfuel.sdbc.base.jdbc.statement

import java.io.{InputStream, Reader}
import java.lang
import java.net.URL
import java.nio.ByteBuffer
import java.sql.{Date => JdbcDate, Array => _, _}
import java.time._
import java.util.{Date, UUID}
import scala.xml.NodeSeq
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

  implicit val BoxedLongParameter: Parameter[lang.Long] =
    DerivedParameter[lang.Long, Long]

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

  implicit val BoxedIntParameter: Parameter[lang.Integer] =
    DerivedParameter[Integer, Int]

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

  implicit val BoxedShortParameter: Parameter[lang.Short] =
    DerivedParameter[lang.Short, Short]

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

  implicit val BoxedByteParameter: Parameter[lang.Byte] =
    DerivedParameter[lang.Byte, Byte]

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

  implicit val ByteBufferParameter: Parameter[ByteBuffer] =
    DerivedParameter[ByteBuffer, ByteVector](ByteVector.apply, ByteVectorParameter)

  implicit val ArrayByteParameter: Parameter[Array[Byte]] =
    DerivedParameter[Array[Byte], ByteVector](ByteVector.apply, ByteVectorParameter)

  implicit val SeqByteParameter: Parameter[Seq[Byte]] =
    DerivedParameter[Seq[Byte], ByteVector](ByteVector.apply, ByteVectorParameter)

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

  implicit val BoxedFloatParameter: Parameter[lang.Float] =
    DerivedParameter[lang.Float, Float]

}

trait DoubleParameter {
  self: ParameterValue =>

  implicit val DoubleParameter: Parameter[Double] =
    new Parameter[Double] {
      override val set: Double => (PreparedStatement, Int) => PreparedStatement = {
        (value) => (statement, parameterIndex) =>
          statement.setDouble(parameterIndex + 1, value)
          statement
      }
    }

  implicit val BoxedDoubleParameter: Parameter[lang.Double] =
    DerivedParameter[lang.Double, Double]

}

trait BigDecimalParameter {
  self: ParameterValue =>

  implicit val JavaBigDecimalParameter: Parameter[java.math.BigDecimal] =
    new Parameter[java.math.BigDecimal] {
      override val set: java.math.BigDecimal => (PreparedStatement, Int) => PreparedStatement = {
        (value) => (statement, parameterIndex) =>
          statement.setBigDecimal(parameterIndex + 1, value)
          statement
      }
    }

  implicit val BigDecimalParameter: Parameter[BigDecimal] =
    new DerivedParameter[BigDecimal] {
      override type B = java.math.BigDecimal
      override val conversion: BigDecimal => B = _.underlying()
      override val baseParameter: Parameter[java.math.BigDecimal] = JavaBigDecimalParameter
    }

}

trait DateParameter {
  self: ParameterValue =>

  implicit val DateParameter: Parameter[Date] =
    new Parameter[Date] {
      override val set: Date => (PreparedStatement, Int) => PreparedStatement = {
        (value) => (statement, parameterIndex) =>
          value match {
            case jdbcDate: JdbcDate =>
              statement.setDate(parameterIndex + 1, jdbcDate)
            case jdbcTime: Time =>
              statement.setTime(parameterIndex + 1, jdbcTime)
            case timestamp: Timestamp =>
              statement.setTimestamp(parameterIndex + 1, timestamp)
            case otherwise =>
              statement.setDate(parameterIndex + 1, new JdbcDate(otherwise.getTime))
          }
          statement
      }
    }

  implicit val LocalDateParameter: Parameter[LocalDate] =
    new DerivedParameter[LocalDate] {
      override type B = JdbcDate
      override val conversion: LocalDate => B = JdbcDate.valueOf
      override val baseParameter: Parameter[B] = DateParameter
    }

  implicit val LocalTimeParameter: Parameter[LocalTime] =
    new DerivedParameter[LocalTime] {
      override type B = Time
      override val conversion: LocalTime => B = Time.valueOf
      override val baseParameter: Parameter[B] = DateParameter
    }

  implicit val InstantParameter: Parameter[Instant] =
    new DerivedParameter[Instant] {
      override type B = Timestamp
      override val conversion: Instant => B = Timestamp.from
      override val baseParameter: Parameter[B] = DateParameter
    }

  implicit val LocalDateTimeParameter: Parameter[LocalDateTime] =
    new DerivedParameter[LocalDateTime] {
      override type B = Timestamp
      override val conversion: LocalDateTime => B = Timestamp.valueOf
      override val baseParameter: Parameter[B] = DateParameter
    }

}

trait BooleanParameter {
  self: ParameterValue =>

  implicit val BooleanParameter: Parameter[Boolean] =
    new Parameter[Boolean] {
      override val set: Boolean => (PreparedStatement, Int) => PreparedStatement = {
        (value) => (statement, parameterIndex) =>
          statement.setBoolean(parameterIndex + 1, value)
          statement
      }
    }

  implicit val BoxedBooleanParameter: Parameter[lang.Boolean] =
    DerivedParameter[lang.Boolean, Boolean]

}

trait StringParameter {
  self: ParameterValue =>

  implicit val StringParameter: Parameter[String] =
    new Parameter[String] {
      override val set: String => (PreparedStatement, Int) => PreparedStatement = {
        (value) => (statement, parameterIndex) =>
          statement.setString(parameterIndex + 1, value)
          statement
      }
    }

}

trait ReaderParameter {
  self: ParameterValue =>

  implicit val ReaderParameter: Parameter[Reader] =
    new Parameter[Reader] {
      override val set: (Reader) => (PreparedStatement, Int) => PreparedStatement = {
        (value: Reader) => (statement: PreparedStatement, parameterIndex: Int) =>
          statement.setCharacterStream(parameterIndex + 1, value)
          statement
      }
    }

}

trait InputStreamParameter {
  self: ParameterValue =>

  implicit val InputStreamParameter: Parameter[InputStream] =
    new Parameter[InputStream] {
      override val set: (InputStream) => (PreparedStatement, Int) => PreparedStatement = {
        (value: InputStream) => (statement: PreparedStatement, parameterIndex: Int) =>
          statement.setBinaryStream(parameterIndex + 1, value)
          statement
      }
    }

}

trait UUIDParameter {
  self: ParameterValue =>

  implicit val UUIDParameter: Parameter[UUID] =
    new Parameter[UUID] {
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

  implicit val URLParameter: Parameter[URL] =
    new Parameter[URL] {
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

  implicit val SQLXMLParameter: Parameter[SQLXML] =
    new Parameter[SQLXML] {
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

  implicit val BlobParameter: Parameter[Blob] =
    new Parameter[Blob] {
      override val set: (Blob) => (PreparedStatement, Int) => PreparedStatement = {
        (value: Blob) => (statement: PreparedStatement, parameterIndex: Int) =>
          statement.setBlob(parameterIndex + 1, value)
          statement
      }
    }

}

