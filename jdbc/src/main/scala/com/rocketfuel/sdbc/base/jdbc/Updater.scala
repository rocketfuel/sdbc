package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.jdbc.resultset.ConnectedRow
import com.rocketfuel.sdbc.base.jdbc.statement.ParameterValue
import java.lang
import java.nio.ByteBuffer
import java.sql.{Time, Date, Timestamp}
import java.io.{InputStream, Reader}
import java.time._
import java.util.UUID
import scala.xml.NodeSeq

import scodec.bits.ByteVector

trait Updater {
  self: ConnectedRow
    with ParameterValue =>

  trait Updater[-T] {

    def update(row: UpdateableRow, columnIndex: Int, x: T): Unit

    def update(row: UpdateableRow, columnLabel: String, x: T): Unit = {
      val columnIndex = row.findColumn(columnLabel)
      update(row, columnIndex, x)
    }

  }

  object Updater {
    def apply[T](implicit updater: Updater[T]): Updater[T] = updater

    implicit def toOptionUpdater[T](implicit updater: Updater[T]): Updater[Option[T]] = {
      new Updater[Option[T]] {
        override def update(row: UpdateableRow, columnIndex: Int, x: Option[T]): Unit = {
          x match {
            case None =>
              row.updateNull(columnIndex)
            case Some(value) =>
              updater.update(row, columnIndex, value)
          }
        }
      }
    }
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
  implicit val NoneUpdater: Updater[None.type] = new Updater[None.type] {
    override def update(row: UpdateableRow, columnIndex: Int, x: None.type): Unit = {
      row.updateNull(columnIndex)
    }
  }

}

trait LongUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val LongUpdater = new Updater[Long] {
    override def update(row: UpdateableRow, columnIndex: Int, x: Long): Unit = {
      row.updateLong(columnIndex, x)
    }
  }

  implicit val BoxedLongUpdater = new Updater[lang.Long] {
    override def update(row: UpdateableRow, columnIndex: Int, x: lang.Long): Unit = {
      LongUpdater.update(row, columnIndex, x.longValue())
    }
  }

}


trait IntUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val IntUpdater = new Updater[Int] {
    override def update(row: UpdateableRow, columnIndex: Int, x: Int): Unit = {
      row.updateInt(columnIndex, x)
    }
  }

  implicit val BoxedIntUpdater = new Updater[lang.Integer] {
    override def update(row: UpdateableRow, columnIndex: Int, x: lang.Integer): Unit = {
      IntUpdater.update(row, columnIndex, x.intValue())
    }
  }

}

trait ShortUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val ShortUpdater = new Updater[Short] {
    override def update(row: UpdateableRow, columnIndex: Int, x: Short): Unit = {
      row.updateShort(columnIndex, x)
    }
  }

  implicit val BoxedShortUpdater = new Updater[lang.Short] {
    override def update(row: UpdateableRow, columnIndex: Int, x: lang.Short): Unit = {
      ShortUpdater.update(row, columnIndex, x.shortValue())
    }
  }
}

trait ByteUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val ByteUpdater = new Updater[Byte] {
    override def update(row: UpdateableRow, columnIndex: Int, x: Byte): Unit = {
      row.updateByte(columnIndex, x)
    }
  }

  implicit val BoxedByteUpdater = new Updater[lang.Byte] {
    override def update(row: UpdateableRow, columnIndex: Int, x: lang.Byte): Unit = {
      ByteUpdater.update(row, columnIndex, x.byteValue())
    }
  }
}

trait BytesUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val ArrayByteUpdater = new Updater[Array[Byte]] {
    override def update(row: UpdateableRow, columnIndex: Int, x: Array[Byte]): Unit = {
      row.updateBytes(columnIndex, x)
    }
  }

  implicit val ByteVectorUpdater = new Updater[ByteVector] {
    override def update(row: UpdateableRow, columnIndex: Int, x: ByteVector): Unit = {
      row.updateBytes(columnIndex, x.toArray)
    }
  }

  implicit val ByteBufferUpdater = new Updater[ByteBuffer] {
    override def update(row: UpdateableRow, columnIndex: Int, x: ByteBuffer): Unit = {
      ArrayByteUpdater.update(row, columnIndex, x.array())
    }
  }

  implicit val SeqByteUpdater = new Updater[Seq[Byte]] {
    override def update(row: UpdateableRow, columnIndex: Int, x: Seq[Byte]): Unit = {
      ArrayByteUpdater.update(row, columnIndex, x.toArray)
    }
  }

}

trait DoubleUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val DoubleUpdater = new Updater[Double] {
    override def update(row: UpdateableRow, columnIndex: Int, x: Double): Unit = {
      row.updateDouble(columnIndex, x)
    }
  }

  implicit val BoxedDoubleUpdater = new Updater[lang.Double] {
    override def update(row: UpdateableRow, columnIndex: Int, x: lang.Double): Unit = {
      DoubleUpdater.update(row, columnIndex, x.doubleValue())
    }
  }
}

trait FloatUpdater {
  self: Updater
    with ConnectedRow =>

  implicit val FloatUpdater = new Updater[Float] {
    override def update(row: UpdateableRow, columnIndex: Int, x: Float): Unit = {
      row.updateFloat(columnIndex, x)
    }
  }

  implicit val BoxedFloatUpdater = new Updater[lang.Float] {
    override def update(row: UpdateableRow, columnIndex: Int, x: lang.Float): Unit = {
      FloatUpdater.update(row, columnIndex, x.floatValue())
    }
  }
}

trait BigDecimalUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val JavaBigDecimalUpdater = new Updater[java.math.BigDecimal] {
    override def update(row: UpdateableRow, columnIndex: Int, x: java.math.BigDecimal): Unit = {
      row.updateBigDecimal(columnIndex, x)
    }
  }

  implicit val ScalaBigDecimalUpdater = new Updater[BigDecimal] {
    override def update(row: UpdateableRow, columnIndex: Int, x: BigDecimal): Unit = {
      JavaBigDecimalUpdater.update(row, columnIndex, x.underlying())
    }
  }

}

trait TimestampUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val TimestampUpdater = new Updater[Timestamp] {
    override def update(row: UpdateableRow, columnIndex: Int, x: Timestamp): Unit = {
      row.updateTimestamp(columnIndex, x)
    }
  }
}

trait DateUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val DateUpdater = new Updater[Date] {
    override def update(row: UpdateableRow, columnIndex: Int, x: Date): Unit = {
      row.updateDate(columnIndex, x)
    }
  }
}

trait TimeUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val TimeUpdater = new Updater[Time] {
    override def update(row: UpdateableRow, columnIndex: Int, x: Time): Unit = {
      row.updateTime(columnIndex, x)
    }
  }
}

trait LocalDateTimeUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val LocalDateTimeUpdater = new Updater[LocalDateTime] {
    override def update(row: UpdateableRow, columnIndex: Int, x: LocalDateTime): Unit = {
      row.updateTimestamp(columnIndex, Timestamp.valueOf(x))
    }
  }
}

trait InstantUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val InstantUpdater = new Updater[Instant] {
    override def update(row: UpdateableRow, columnIndex: Int, x: Instant): Unit = {
      row.updateTimestamp(columnIndex, Timestamp.from(x))
    }
  }
}

trait LocalDateUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val LocalDateUpdater = new Updater[LocalDate] {
    override def update(row: UpdateableRow, columnIndex: Int, x: LocalDate): Unit = {
      row.updateDate(columnIndex, Date.valueOf(x))
    }
  }
}

trait LocalTimeUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val LocalTimeUpdater = new Updater[LocalTime] {
    override def update(row: UpdateableRow, columnIndex: Int, x: LocalTime): Unit = {
      row.updateTime(columnIndex, Time.valueOf(x))
    }
  }
}

trait BooleanUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val BooleanUpdater = new Updater[Boolean] {
    override def update(row: UpdateableRow, columnIndex: Int, x: Boolean): Unit = {
      row.updateBoolean(columnIndex, x)
    }
  }

  implicit val BoxedBooleanUpdater = new Updater[lang.Boolean] {
    override def update(row: UpdateableRow, columnIndex: Int, x: lang.Boolean): Unit = {
      BooleanUpdater.update(row, columnIndex, x.booleanValue())
    }
  }
}

trait StringUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val StringUpdater = new Updater[String] {
    override def update(row: UpdateableRow, columnIndex: Int, x: String): Unit = {
      row.updateString(columnIndex, x)
    }
  }
}

trait UUIDUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val UUIDUpdater = new Updater[UUID] {
    override def update(row: UpdateableRow, columnIndex: Int, x: UUID): Unit = {
      row.updateObject(columnIndex, x)
    }
  }
}


trait InputStreamUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val InputStreamUpdater = new Updater[InputStream] {
    override def update(row: UpdateableRow, columnIndex: Int, x: InputStream): Unit = {
      row.updateBinaryStream(columnIndex, x)
    }
  }
}

trait ReaderUpdater {
  self: Updater
    with ConnectedRow
    with ParameterValue =>

  implicit val ReaderUpdater = new Updater[Reader] {
    override def update(row: UpdateableRow, columnIndex: Int, x: Reader): Unit = {
      row.updateCharacterStream(columnIndex, x)
    }
  }
}

trait XmlUpdater {
  self: Updater
  with ConnectedRow
  with ParameterValue =>

  implicit val NodeSeqUpdater: Updater[NodeSeq] = new Updater[NodeSeq] {
    override def update(row: UpdateableRow, columnIndex: Int, x: NodeSeq): Unit = {
      val sqlxml = row.getStatement.getConnection.createSQLXML()
      sqlxml.setString(x.toString)
      row.updateSQLXML(columnIndex, sqlxml)
    }
  }

}
