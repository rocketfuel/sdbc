package com.rocketfuel.sdbc.base.jdbc

import java.io.{InputStream, Reader}
import java.lang
import java.net.URL
import java.nio.ByteBuffer
import java.sql.{Array => JdbcArray, Date => JdbcDate, _}
import java.time._
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.xml.Node
import scodec.bits.ByteVector

trait LongParameter {
  self: ParameterValue =>

  implicit object LongParameter
    extends PrimaryParameter[Long]
    with SecondaryParameter[lang.Long] {
    override val toParameter: PartialFunction[Any, Any] = {
      case l: Long => l
      case l: lang.Long => l.longValue()
    }
    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case l: Long =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setLong(ix, l)
          statement
    }
  }

}

trait IntParameter {
  self: ParameterValue =>

  implicit object IntParameter
    extends PrimaryParameter[Int]
    with SecondaryParameter[lang.Integer] {
    override val toParameter: PartialFunction[Any, Any] = {
      case l: Int => l
      case l: lang.Integer => l.intValue()
    }
    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case l: Int =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setInt(ix, l)
          statement
    }
  }

}

trait ShortParameter {
  self: ParameterValue =>

  implicit object ShortParameter
    extends PrimaryParameter[Short]
    with SecondaryParameter[lang.Short] {
    override val toParameter: PartialFunction[Any, Any] = {
      case l: Short => l
      case l: lang.Short => l.shortValue()
    }
    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case l: Short =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setShort(ix, l)
          statement
    }
  }

}

trait ByteParameter {
  self: ParameterValue =>

  implicit object ByteParameter
    extends PrimaryParameter[Byte]
    with SecondaryParameter[lang.Byte] {
    override val toParameter: PartialFunction[Any, Any] = {
      case l: Byte => l
      case l: lang.Byte => l.byteValue()
    }
    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case l: Byte =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setByte(ix, l)
          statement
    }
  }

}

trait BytesParameter {
  self: ParameterValue =>

  //We're using ByteVectors, since they're much more easily testable than Array[Byte].
  //IE equality actually works.
  implicit object ByteVectorParameter
    extends PrimaryParameter[ByteVector]
    with SecondaryParameter[Array[Byte]]
    with TertiaryParameter[ByteBuffer] {

    override val toParameter: PartialFunction[Any, Any] = {
      case v: ByteVector => v
      case a: Array[Byte] => ByteVector(a)
      case b: ByteBuffer => ByteVector(b)
    }

    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case v: ByteVector =>
        (statement: Statement, ix: ParameterIndex) =>
          val array = v.toArray
          statement.setBytes(ix, array)
          statement
    }

  }

}

trait FloatParameter {
  self: ParameterValue =>

  implicit object FloatParameter
    extends PrimaryParameter[Float]
    with SecondaryParameter[lang.Float] {
    override val toParameter: PartialFunction[Any, Any] = {
      case l: Float => l
      case l: lang.Float => l.floatValue()
    }
    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case l: Float =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setFloat(ix, l)
          statement
    }
  }

}

trait DoubleParameter {
  self: ParameterValue =>

  implicit object DoubleParameter
    extends PrimaryParameter[Double]
    with SecondaryParameter[lang.Double] {
    override val toParameter: PartialFunction[Any, Any] = {
      case l: Double => l
      case l: lang.Double => l.doubleValue()
    }
    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case l: Double =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setDouble(ix, l)
          statement
    }
  }

}

trait BigDecimalParameter {
  self: ParameterValue =>

  implicit object BigDecimalParameter
    extends PrimaryParameter[BigDecimal]
    with SecondaryParameter[java.math.BigDecimal] {
    override val toParameter: PartialFunction[Any, Any] = {
      case l: BigDecimal => l.underlying
      case l: java.math.BigDecimal => l
    }
    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case l: java.math.BigDecimal =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setBigDecimal(ix, l)
          statement
    }
  }

}

trait TimestampParameter {
  self: ParameterValue =>

  implicit object TimestampParameter
    extends PrimaryParameter[Timestamp]
    with SecondaryParameter[java.util.Date]
    with TertiaryParameter[Instant]
    with QuaternaryParameter[LocalDateTime] {
    override val toParameter: PartialFunction[Any, Any] = {
      case i: Timestamp => i
      case i: java.util.Date => new Timestamp(i.getTime)
      case i: Instant => Timestamp.from(i)
      case i: LocalDateTime => Timestamp.valueOf(i)
    }
    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case i: Timestamp =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setTimestamp(ix, i)
          statement
    }
  }

}

trait DateParameter {
  self: ParameterValue =>

  implicit object DateParameter
    extends PrimaryParameter[JdbcDate]
    with SecondaryParameter[LocalDate] {

    override val toParameter: PartialFunction[Any, Any] = {
      case d: JdbcDate => d
      case l: java.time.LocalDate =>
        java.sql.Date.valueOf(l)
    }
    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case i: JdbcDate =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setDate(ix, i)
          statement
    }

  }

}

trait TimeParameter {
  self: ParameterValue =>

  implicit object TimeParameter
    extends PrimaryParameter[java.sql.Time]
    with SecondaryParameter[LocalTime] {
    override val toParameter: PartialFunction[Any, Any] = {
      case d: java.sql.Time => d
      case l: java.time.LocalTime =>
        java.sql.Time.valueOf(l)
    }
    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case i: Time =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setTime(ix, i)
          statement
    }
  }

}

trait BooleanParameter {
  self: ParameterValue =>

  implicit object BooleanParameter
    extends PrimaryParameter[Boolean]
    with SecondaryParameter[lang.Boolean] {

    override val toParameter: PartialFunction[Any, Any] = {
      case l: Boolean => l
      case l: lang.Boolean => l.booleanValue()
    }

    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case l: Boolean =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setBoolean(ix, l)
          statement
    }

  }

}

trait StringParameter {
  self: ParameterValue =>

  implicit object StringParameter
    extends PrimaryParameter[String] {

    override val toParameter: PartialFunction[Any, Any] = {
      case l: String => l
    }

    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case l: String =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setString(ix, l)
          statement
    }

  }

}

trait ReaderParameter {
  self: ParameterValue =>

  implicit object ReaderParameter
    extends PrimaryParameter[Reader] {

    override val toParameter: PartialFunction[Any, Any] = {
      case l: Reader => l
    }

    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case l: Reader =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setCharacterStream(ix, l)
          statement
    }

  }

}

trait InputStreamParameter {
  self: ParameterValue =>

  implicit object InputStreamParameter
    extends PrimaryParameter[InputStream] {

    override val toParameter: PartialFunction[Any, Any] = {
      case l: InputStream => l
    }

    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case l: InputStream =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setBinaryStream(ix, l)
          statement
    }

  }

}

trait UUIDParameter {
  self: ParameterValue =>

  implicit object UUIDParameter
    extends PrimaryParameter[UUID] {

    override val toParameter: PartialFunction[Any, Any] = {
      case l: UUID => l
    }

    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case l: UUID =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setObject(ix, l)
          statement
    }

  }
}

//This is left out of the defaults, since no one seems to support it.
//jTDS supports it, but SQL Server doesn't have a url type.
trait URLParameter {
  self: ParameterValue =>

  implicit object URLParameter
    extends PrimaryParameter[URL] {

    override val toParameter: PartialFunction[Any, Any] = {
      case l: URL => l
    }

    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case l: URL =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setURL(ix, l)
          statement
    }

  }

}

trait ArrayParameter {
  self: ParameterValue =>

  implicit object ArrayParameter
    extends PrimaryParameter[JdbcArray] {

    override val toParameter: PartialFunction[Any, Any] = {
      case l: JdbcArray => l
    }

    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case l: JdbcArray =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setArray(ix, l)
          statement
    }

  }

}

trait XMLParameter {
  self: ParameterValue =>

  implicit object XmlParameter
    extends PrimaryParameter[Node]
    with SecondaryParameter[SQLXML] {

    override val toParameter: PartialFunction[Any, Any] = {
      case l: Node => l
      case s: SQLXML => s
    }

    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case l: Node =>
        (statement: Statement, ix: ParameterIndex) =>
          val sqlxml = statement.getConnection.createSQLXML()
          sqlxml.setString(l.toString)
          statement.setSQLXML(ix, sqlxml)
          statement
      case sqlxml: SQLXML =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setSQLXML(ix, sqlxml)
          statement
    }

  }

}

trait BlobParameter {
  self: ParameterValue =>

  implicit object BlobParameter
    extends PrimaryParameter[Blob] {

    override val toParameter: PartialFunction[Any, Any] = {
      case b: Blob => b
    }

    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {
      case b: Blob =>
        (statement: Statement, ix: ParameterIndex) =>
          statement.setBlob(ix, b)
          statement
    }

  }

}

trait OffsetDateTimeAsTimestampParameter {
  self: ParameterValue
    with TimestampParameter =>

  implicit object OffsetDateTimeParameter
    extends PrimaryParameter[OffsetDateTime] {

    override val toParameter: PartialFunction[Any, Any] = {
      case l: OffsetDateTime =>
        Timestamp.from(l.toInstant)
    }

    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] =
      PartialFunction.empty
  }

}

trait OffsetDateTimeAsStringParameter {
  self: ParameterValue
    with StringParameter =>

  val offsetDateTimeFormatter: DateTimeFormatter

  implicit object OffsetDateTimeParameter
    extends PrimaryParameter[OffsetDateTime] {

    override val toParameter: PartialFunction[Any, Any] = {
      case l: OffsetDateTime =>
        val formatted = offsetDateTimeFormatter.format(l)
        formatted
    }

    override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] =
      PartialFunction.empty

  }

}
