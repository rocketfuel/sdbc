package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.jdbc.resultset.ConnectedRow
import com.rocketfuel.sdbc.base.jdbc.statement.ParameterValue
import java.lang
import java.nio.ByteBuffer
import java.sql.{Date, Time, Timestamp}
import java.io.{InputStream, Reader}
import java.time._
import java.util.UUID
import scala.xml._
import scodec.bits.ByteVector

trait Updater {
  self: DBMS with SelectForUpdate with Connection =>

  trait Updater[-T] extends ((UpdatableRow, Int, T) => Unit)

  object Updater {
    def apply[T](implicit updater: Updater[T]): Updater[T] = updater

    implicit def ofFunction[A](f: (UpdatableRow, Int, A) => Unit): Updater[A] =
      new Updater[A] {
        override def apply(row: UpdatableRow, columnIndex: Int, x: A): Unit =
          f(row, columnIndex, x)
      }

    implicit def toOptionUpdater[T](implicit updater: Updater[T]): Updater[Option[T]] = {
      (row: UpdatableRow, columnIndex: Int, x: Option[T]) =>
        x match {
          case None =>
            row.updateNull(columnIndex)
          case Some(value) =>
            updater(row, columnIndex, value)
        }
    }

    implicit def derived[A, B](implicit converter: A => B, updater: Updater[B]): Updater[A] = {
      (row: UpdatableRow, columnIndex: Int, x: A) =>
        updater(row, columnIndex, converter(x))
    }

    implicit def converted[A, B](converter: A => B)(implicit updater: Updater[B]): Updater[A] = {
      derived[A, B](converter, updater)
    }

    def cast[A <: B, B](implicit updater: Updater[B]): Updater[A] = {
      (row: UpdatableRow, columnIndex: Int, x: A) =>
        updater(row, columnIndex, x)
    }

    def toString[A](implicit updater: Updater[String]): Updater[A] =
      (a: A) => a.toString
  }

  /**
    * This implicit is used if None is used on the right side of an update.
    *
    * {{{
    *   val row: MutableRow = ???
    *
    *   row("columnName") = None
    * }}}
    */
  implicit val NoneUpdater: Updater[None.type] =
    (row: UpdatableRow, columnIndex: Int, _: None.type) =>
      row.updateNull(columnIndex)

}

trait LongUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue =>

  implicit val LongUpdater: Updater[Long] =
    _.updateLong(_, _)

  implicit val BoxedLongUpdater: Updater[lang.Long] =
    Updater.derived[lang.Long, Long]

}

trait IntUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue =>

  implicit val IntUpdater: Updater[Int] =
    _.updateInt(_, _)

  implicit val BoxedIntUpdater: Updater[lang.Integer] =
    Updater.derived[lang.Integer, Int]

}

trait ShortUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue =>

  implicit val ShortUpdater: Updater[Short] =
    _.updateShort(_, _)

  implicit val BoxedShortUpdater: Updater[lang.Short] =
    Updater.derived[lang.Short, Short]

}

trait ByteUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue =>

  implicit val ByteUpdater: Updater[Byte] =
    _.updateByte(_, _)

  implicit val BoxedByteUpdater: Updater[lang.Byte] =
    Updater.derived[lang.Byte, Byte]

}

trait BytesUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue =>

  implicit val ArrayByteUpdater: Updater[Array[Byte]] =
    _.updateBytes(_, _)

  implicit val ByteVectorUpdater: Updater[ByteVector] =
    (row: UpdatableRow, columnIndex: Int, x: ByteVector) =>
      row.updateBytes(columnIndex, x.toArray)

  implicit val ByteBufferUpdater: Updater[ByteBuffer] =
    (row: UpdatableRow, columnIndex: Int, x: ByteBuffer) =>
      ArrayByteUpdater(row, columnIndex, x.array())

  implicit val SeqByteUpdater: Updater[Seq[Byte]] =
    (row: UpdatableRow, columnIndex: Int, x: Seq[Byte]) =>
      ArrayByteUpdater(row, columnIndex, x.toArray)

}

trait DoubleUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue =>

  implicit val DoubleUpdater: Updater[Double] =
    _.updateDouble(_, _)

  implicit val BoxedDoubleUpdater: Updater[lang.Double] =
    Updater.derived[lang.Double, Double]
}

trait FloatUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow =>

  implicit val FloatUpdater: Updater[Float] =
    _.updateFloat(_, _)

  implicit val BoxedFloatUpdater: Updater[lang.Float] =
    Updater.derived[lang.Float, Float]

}

trait BigDecimalUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue =>

  implicit val JavaBigDecimalUpdater: Updater[java.math.BigDecimal] =
    _.updateBigDecimal(_, _)

  implicit val ScalaBigDecimalUpdater: Updater[BigDecimal] =
    (b: BigDecimal) => b.underlying()

}

trait TimestampUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue =>

  implicit val TimestampUpdater: Updater[Timestamp] =
    _.updateTimestamp(_, _)

}

trait DateUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue =>

  implicit val DateUpdater: Updater[Date] =
    _.updateDate(_, _)

}

trait TimeUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue =>

  implicit val TimeUpdater: Updater[Time] =
    _.updateTime(_, _)

}

trait LocalDateTimeUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue
    with TimestampUpdater =>

  implicit val LocalDateTimeUpdater: Updater[LocalDateTime] =
    (t: LocalDateTime) => Timestamp.valueOf(t)

}

trait InstantUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue
    with TimestampUpdater =>

  implicit val InstantUpdater: Updater[Instant] =
    (t: Instant) => Timestamp.from(t)

}

trait LocalDateUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue
    with DateUpdater =>

  implicit val LocalDateUpdater: Updater[LocalDate] =
    (t: LocalDate) => Date.valueOf(t)

}

trait LocalTimeUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue
    with TimeUpdater =>

  implicit val LocalTimeUpdater: Updater[LocalTime] =
    (t: LocalTime) => Time.valueOf(t)

}

trait BooleanUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue =>

  implicit val BooleanUpdater: Updater[Boolean] =
    _.updateBoolean(_, _)

  implicit val BoxedBooleanUpdater: Updater[lang.Boolean] =
    Updater.derived[lang.Boolean, Boolean]

}

trait StringUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue =>

  implicit val StringUpdater: Updater[String] =
    _.updateString(_, _)

}

trait UUIDUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue =>

  implicit val UUIDUpdater: Updater[UUID] =
    _.updateObject(_, _)

}

trait InputStreamUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue =>

  implicit val InputStreamUpdater: Updater[InputStream] =
    _.updateBinaryStream(_, _)

}

trait ReaderUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue =>

  implicit val ReaderUpdater: Updater[Reader] =
    _.updateCharacterStream(_, _)

}

trait XmlUpdater {
  self: Updater
    with SelectForUpdate
    with ConnectedRow
    with ParameterValue =>

  implicit val XmlElemUpdater: Updater[Elem] = {
    (row: UpdatableRow, columnIndex: Int, x: Node) =>
      val sqlxml = row.getStatement.getConnection.createSQLXML()
      sqlxml.setString(x.toString)
      row.updateSQLXML(columnIndex, sqlxml)
  }

}
