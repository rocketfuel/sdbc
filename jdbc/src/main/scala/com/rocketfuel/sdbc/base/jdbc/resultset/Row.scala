package com.rocketfuel.sdbc.base.jdbc.resultset

import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.jdbc.DBMS
import java.math.BigDecimal
import java.net.URL
import java.sql.{Array => JdbcArray, _}

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

    def apply[T](columnIndex: Index)(implicit getter: CompositeGetter[this.type, T]): T = {
      getter(this, columnIndex)
    }

    def wasNull: Boolean

    def getTimestamp(columnIndex: Int): Timestamp

    def getTimestamp(columnLabel: String): Timestamp

    def getDouble(columnIndex: Int): Double

    def getDouble(columnLabel: String): Double

    def getArray(columnIndex: Int): JdbcArray

    def getArray(columnLabel: String): JdbcArray

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
    private[sdbc] def toSeq(
      row: ResultSet
    ): IndexedSeq[Option[Any]] = {
      IndexedSeq.tabulate(row.getMetaData.getColumnCount) { ix =>
        Option(row.getObject(ix + 1)).map(base.unbox)
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
      columnNames.zipWithIndex.toMap
    }
  }

}
