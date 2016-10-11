package com.rocketfuel.sdbc.base.jdbc.resultset

import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.jdbc.DBMS
import java.io.{InputStream, Reader}
import java.math.BigDecimal
import java.net.URL
import java.sql.{Array => JdbcArray, _}
import scala.collection.immutable.TreeMap
import scodec.bits.ByteVector

trait Row extends base.Index {
  self: DBMS =>

  abstract class Row private[jdbc]() extends RowIndexOps {

    override def columnCount: Int = getMetaData.getColumnCount

    def toSeq: IndexedSeq[Option[Any]]

    def toMap: Map[String, Option[Any]]

    /**
      * The index of the row in the ResultSet.
      *
      * @return
      */
    def getRow: Int

    def getMetaData: ResultSetMetaData

    def wasNull: Boolean

    def getTimestamp(columnIndex: Int): Timestamp

    def getTimestamp(columnLabel: String): Timestamp

    def getDouble(columnIndex: Int): Double

    def getDouble(columnLabel: String): Double

    def getURL(columnIndex: Int): URL

    def getURL(columnLabel: String): URL

    def getBigDecimal(columnIndex: Int): BigDecimal

    def getBigDecimal(columnLabel: String): BigDecimal

    def getFloat(columnIndex: Int): Float

    def getFloat(columnLabel: String): Float

    def getLong(columnIndex: Int): Long

    def getLong(columnLabel: String): Long

    def getTime(columnIndex: Int): Time

    def getTime(columnLabel: String): Time

    def getByte(columnIndex: Int): Byte

    def getByte(columnLabel: String): Byte

    def getBoolean(columnIndex: Int): Boolean

    def getBoolean(columnLabel: String): Boolean

    def getShort(columnIndex: Int): Short

    def getShort(columnLabel: String): Short

    def getDate(columnIndex: Int): Date

    def getDate(columnLabel: String): Date

    def getSQLXML(columnIndex: Int): SQLXML

    def getSQLXML(columnLabel: String): SQLXML

    def getInt(columnIndex: Int): Int

    def getInt(columnLabel: String): Int

    def getBytes(columnIndex: Int): Array[Byte]

    def getBytes(columnLabel: String): Array[Byte]

    def getString(columnIndex: Int): String

    def getString(columnLabel: String): String

    def getObject(columnIndex: Int): AnyRef

    def getObject(columnLabel: String): AnyRef

  }

  object Row {
    private def readerToIterator(bufferSize: Int, r: Reader): Iterator[String] = {
      val buffer = new Array[Char](bufferSize)
      Iterator.continually {
        val charsRead = r.read(buffer)
        if (charsRead > 0)
          new String(buffer, 0, charsRead)
        else null
      } takeWhile(_ != null)
    }

    private def fromClob(c: Clob): String = {
      val r = c.getCharacterStream
      val sb = new StringBuilder
      readerToIterator(4096, r).foreach(sb.append)
      sb.toString
    }

    private def inputStreamToIterator(bufferSize: Int, i: InputStream): Iterator[ByteVector] = {
      val buffer = new Array[Byte](bufferSize)
      Iterator.continually {
        val bytesRead = i.read(buffer)
        if (bytesRead > 0)
          if (bytesRead == bufferSize)
            ByteVector(buffer)
          else ByteVector(buffer.take(bytesRead))
        else null
      } takeWhile(_ != null)
    }

    private def fromBlob(b: Blob): ByteVector = {
      inputStreamToIterator(4096, b.getBinaryStream).foldRight(ByteVector.empty) {
        case (accum, toAppend) =>
          accum ++ toAppend
      }
    }

    private[sdbc] def toSeq(
      row: ResultSet
    ): IndexedSeq[Option[Any]] = {
      IndexedSeq.tabulate(row.getMetaData.getColumnCount) { ix =>
        Option(row.getObject(ix + 1)) map {
          case c: Clob =>
            fromClob(c)
          case b: Blob =>
            fromBlob(b)
          case otherwise =>
            base.unbox(otherwise)
        }
      }
    }

    private[sdbc] def toMap(
      toSeq: IndexedSeq[Option[Any]],
      getMetaData: ResultSetMetaData
    ): Map[String, Option[Any]] = {
      toSeq.zipWithIndex.map {
        case (value, ix) =>
          val columnName = getMetaData.getColumnName(ix)
          columnName -> value
      }.toMap
    }

    private[sdbc] def columnNames(resultSetMetaData: ResultSetMetaData): IndexedSeq[String] = {
      IndexedSeq.tabulate(resultSetMetaData.getColumnCount) { ix =>
        resultSetMetaData.getColumnName(ix + 1)
      }
    }

    private[sdbc] def columnIndexes(columnNames: IndexedSeq[String]): Map[String, Int] = {
      TreeMap(columnNames.zipWithIndex: _*)(new Ordering[String] {
        override def compare(x: String, y: String): Int = x.compareToIgnoreCase(y)
      })
    }

  }

}
