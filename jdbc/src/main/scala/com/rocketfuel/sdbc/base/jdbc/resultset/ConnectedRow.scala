package com.rocketfuel.sdbc.base.jdbc.resultset

import com.rocketfuel.sdbc.base.jdbc.DBMS
import java.io.{InputStream, Reader}
import java.math.BigDecimal
import java.net.URL
import java.sql.{Array => JdbcArray, _}
import java.util
import java.util.Calendar

trait ConnectedRow {
  self: DBMS =>

  class ConnectedRow private[sdbc](
    val underlying: ResultSet,
    override val columnNames: IndexedSeq[String],
    override val columnIndexes: Map[String, Int]
  ) extends Row()
    with ResultSet {

    def apply[T](columnIndex: Index)(implicit getter: CompositeGetter[T]): T = {
      getter(this, columnIndex)
    }

    override def toSeq: IndexedSeq[Option[Any]] = Row.toSeq(underlying)

    override def toMap: Map[String, Option[Any]] = Row.toMap(toSeq, getMetaData)

    def getType: Int = underlying.getType

    def isBeforeFirst: Boolean = underlying.isBeforeFirst

    override def getTimestamp(columnIndex: Int): Timestamp = underlying.getTimestamp(columnIndex + 1)

    override def getTimestamp(columnLabel: String): Timestamp = underlying.getTimestamp(columnLabel: String)

    def getTimestamp(columnIndex: Int, cal: Calendar): Timestamp = underlying.getTimestamp(columnIndex + 1, cal: Calendar)

    def getTimestamp(columnLabel: String, cal: Calendar): Timestamp = underlying.getTimestamp(columnLabel: String, cal: Calendar)

    def clearWarnings(): Unit = underlying.clearWarnings()

    def isAfterLast: Boolean = underlying.isAfterLast

    def getBinaryStream(columnIndex: Int): InputStream = underlying.getBinaryStream(columnIndex + 1)

    def getBinaryStream(columnLabel: String): InputStream = underlying.getBinaryStream(columnLabel: String)

    def isLast: Boolean = underlying.isLast

    def getNClob(columnIndex: Int): NClob = underlying.getNClob(columnIndex + 1)

    def getNClob(columnLabel: String): NClob = underlying.getNClob(columnLabel: String)

    def getCharacterStream(columnIndex: Int): Reader = underlying.getCharacterStream(columnIndex + 1)

    def getCharacterStream(columnLabel: String): Reader = underlying.getCharacterStream(columnLabel: String)

    override def getDouble(columnIndex: Int): Double = underlying.getDouble(columnIndex + 1)

    override def getDouble(columnLabel: String): Double = underlying.getDouble(columnLabel: String)

    override def getArray(columnIndex: Int): JdbcArray = underlying.getArray(columnIndex + 1)

    override def getArray(columnLabel: String): JdbcArray = underlying.getArray(columnLabel)

    def isFirst: Boolean = underlying.isFirst

    override def getURL(columnIndex: Int): URL = underlying.getURL(columnIndex + 1)

    override def getURL(columnLabel: String): URL = underlying.getURL(columnLabel: String)

    override def getMetaData: ResultSetMetaData = underlying.getMetaData

    def getRowId(columnIndex: Int): RowId = underlying.getRowId(columnIndex + 1)

    def getRowId(columnLabel: String): RowId = underlying.getRowId(columnLabel: String)

    override def getBigDecimal(columnIndex: Int): BigDecimal = underlying.getBigDecimal(columnIndex + 1)

    override def getBigDecimal(columnLabel: String): BigDecimal = underlying.getBigDecimal(columnLabel: String)

    override def getFloat(columnIndex: Int): Float = underlying.getFloat(columnIndex + 1)

    override def getFloat(columnLabel: String): Float = underlying.getFloat(columnLabel: String)

    def getClob(columnIndex: Int): Clob = underlying.getClob(columnIndex + 1)

    def getClob(columnLabel: String): Clob = underlying.getClob(columnLabel: String)

    /**
      * Returns the current row number if the underlying ResultSet supports
      * `ResultSet#getRow()`.
      *
      * Unlike JDBC, SDBC's getRow is 0-indexed.
      *
      * @return
      */
    override def getRow: Int = {
      underlying.getRow - 1
    }

    override def getLong(columnIndex: Int): Long = underlying.getLong(columnIndex + 1)

    override def getLong(columnLabel: String): Long = underlying.getLong(columnLabel: String)

    def getHoldability: Int = underlying.getHoldability

    def refreshRow(): Unit = underlying.refreshRow()

    def getNString(columnIndex: Int): String = underlying.getNString(columnIndex + 1)

    def getNString(columnLabel: String): String = underlying.getNString(columnLabel: String)

    def getConcurrency: Int = underlying.getConcurrency

    def getFetchSize: Int = underlying.getFetchSize

    def setFetchSize(rows: Int): Unit = underlying.setFetchSize(rows: Int)

    override def getTime(columnIndex: Int): Time = underlying.getTime(columnIndex + 1)

    override def getTime(columnLabel: String): Time = underlying.getTime(columnLabel: String)

    def getTime(columnIndex: Int, cal: Calendar): Time = underlying.getTime(columnIndex + 1, cal: Calendar)

    def getTime(columnLabel: String, cal: Calendar): Time = underlying.getTime(columnLabel: String, cal: Calendar)

    override def getByte(columnIndex: Int): Byte = underlying.getByte(columnIndex + 1)

    override def getByte(columnLabel: String): Byte = underlying.getByte(columnLabel: String)

    override def getBoolean(columnIndex: Int): Boolean = underlying.getBoolean(columnIndex + 1)

    override def getBoolean(columnLabel: String): Boolean = underlying.getBoolean(columnLabel: String)

    def getFetchDirection: Int = underlying.getFetchDirection

    def getAsciiStream(columnIndex: Int): InputStream = underlying.getAsciiStream(columnIndex + 1)

    def getAsciiStream(columnLabel: String): InputStream = underlying.getAsciiStream(columnLabel: String)

    override def getObject(columnIndex: Int): AnyRef = underlying.getObject(columnIndex + 1)

    override def getObject(columnLabel: String): AnyRef = underlying.getObject(columnLabel: String)

    def getObject(columnIndex: Int, map: util.Map[String, Class[_]]): AnyRef = underlying.getObject(columnIndex + 1, map: util.Map[String, Class[_]])

    def getObject(columnLabel: String, map: util.Map[String, Class[_]]): AnyRef = underlying.getObject(columnLabel: String, map: util.Map[String, Class[_]])

    def getObject[T](columnIndex: Int, `type`: Class[T]): T = underlying.getObject[T](columnIndex + 1, `type`: Class[T])

    def getObject[T](columnLabel: String, `type`: Class[T]): T = underlying.getObject[T](columnLabel: String, `type`: Class[T])

    override def getShort(columnIndex: Int): Short = underlying.getShort(columnIndex + 1)

    override def getShort(columnLabel: String): Short = underlying.getShort(columnLabel: String)

    def getNCharacterStream(columnIndex: Int): Reader = underlying.getNCharacterStream(columnIndex + 1)

    def getNCharacterStream(columnLabel: String): Reader = underlying.getNCharacterStream(columnLabel: String)

    def close(): Unit = underlying.close()

    def wasNull: Boolean = underlying.wasNull

    def getRef(columnIndex: Int): Ref = underlying.getRef(columnIndex + 1)

    def getRef(columnLabel: String): Ref = underlying.getRef(columnLabel: String)

    def isClosed: Boolean = underlying.isClosed

    def findColumn(columnLabel: String): Int = {
      underlying.findColumn(columnLabel: String) - 1
    }

    def getWarnings: SQLWarning = underlying.getWarnings

    override def getDate(columnIndex: Int): Date = underlying.getDate(columnIndex + 1)

    override def getDate(columnLabel: String): Date = underlying.getDate(columnLabel: String)

    def getDate(columnIndex: Int, cal: Calendar): Date = underlying.getDate(columnIndex + 1, cal: Calendar)

    def getDate(columnLabel: String, cal: Calendar): Date = underlying.getDate(columnLabel: String, cal: Calendar)

    def getCursorName: String = underlying.getCursorName

    def getStatement: java.sql.Statement = underlying.getStatement

    override def getSQLXML(columnIndex: Int): SQLXML = underlying.getSQLXML(columnIndex + 1)

    override def getSQLXML(columnLabel: String): SQLXML = underlying.getSQLXML(columnLabel: String)

    override def getInt(columnIndex: Int): Int = underlying.getInt(columnIndex + 1)

    override def getInt(columnLabel: String): Int = underlying.getInt(columnLabel: String)

    def getBlob(columnIndex: Int): Blob = underlying.getBlob(columnIndex + 1)

    def getBlob(columnLabel: String): Blob = underlying.getBlob(columnLabel: String)

    override def getBytes(columnIndex: Int): Array[Byte] = underlying.getBytes(columnIndex + 1)

    override def getBytes(columnLabel: String): Array[Byte] = underlying.getBytes(columnLabel: String)

    override def getString(columnIndex: Int): String = underlying.getString(columnIndex + 1)

    override def getString(columnLabel: String): String = underlying.getString(columnLabel: String)

    def updateArray(columnIndex: Int, x: JdbcArray): Unit = underlying.updateArray(columnIndex + 1, x)

    def updateArray(columnLabel: String, x: JdbcArray): Unit = underlying.updateArray(columnLabel: String, x)

    def updateAsciiStream(columnIndex: Int, x: InputStream): Unit = underlying.updateAsciiStream(columnIndex + 1, x)

    def updateAsciiStream(columnLabel: String, x: InputStream): Unit = underlying.updateAsciiStream(columnLabel, x)

    def updateAsciiStream(columnIndex: Int, x: InputStream, length: Int): Unit = underlying.updateAsciiStream(columnIndex + 1, x, length)

    def updateAsciiStream(columnLabel: String, x: InputStream, length: Int): Unit = underlying.updateAsciiStream(columnLabel, x, length)

    def updateAsciiStream(columnIndex: Int, x: InputStream, length: Long): Unit = underlying.updateAsciiStream(columnIndex + 1, x, length)

    def updateAsciiStream(columnLabel: String, x: InputStream, length: Long): Unit = underlying.updateAsciiStream(columnLabel, x, length)

    def updateBigDecimal(columnIndex: Int, x: BigDecimal): Unit = underlying.updateBigDecimal(columnIndex + 1, x)

    def updateBigDecimal(columnLabel: String, x: BigDecimal): Unit = underlying.updateBigDecimal(columnLabel, x)

    def updateBigDecimal(columnIndex: Int, x: scala.BigDecimal): Unit = updateBigDecimal(columnIndex + 1, x.underlying())

    def updateBigDecimal(columnLabel: String, x: scala.BigDecimal): Unit = updateBigDecimal(columnLabel, x.underlying())

    def updateBinaryStream(columnIndex: Int, x: InputStream): Unit = underlying.updateBinaryStream(columnIndex + 1, x)

    def updateBinaryStream(columnLabel: String, x: InputStream): Unit = underlying.updateBinaryStream(columnLabel, x)

    def updateBinaryStream(columnIndex: Int, x: InputStream, length: Int): Unit = underlying.updateBinaryStream(columnIndex + 1, x, length)

    def updateBinaryStream(columnLabel: String, x: InputStream, length: Int): Unit = underlying.updateBinaryStream(columnLabel, x, length)

    def updateBinaryStream(columnIndex: Int, x: InputStream, length: Long): Unit = underlying.updateBinaryStream(columnIndex + 1, x, length)

    def updateBinaryStream(columnLabel: String, x: InputStream, length: Long): Unit = underlying.updateBinaryStream(columnLabel, x, length)

    def updateBlob(columnIndex: Int, x: InputStream, length: Long): Unit = underlying.updateBlob(columnIndex + 1, x, length)

    def updateBlob(columnLabel: String, x: InputStream, length: Long): Unit = underlying.updateBlob(columnLabel, x, length)

    def updateBlob(columnIndex: Int, x: InputStream): Unit = underlying.updateBlob(columnIndex + 1, x)

    def updateBlob(columnLabel: String, x: InputStream): Unit = underlying.updateBlob(columnLabel, x)

    def updateBlob(columnIndex: Int, x: java.sql.Blob): Unit = underlying.updateBlob(columnIndex + 1, x)

    def updateBlob(columnLabel: String, x: java.sql.Blob): Unit = underlying.updateBlob(columnLabel, x)

    def updateBoolean(columnIndex: Int, x: Boolean): Unit = underlying.updateBoolean(columnIndex + 1, x)

    def updateBoolean(columnLabel: String, x: Boolean): Unit = underlying.updateBoolean(columnLabel, x)

    def updateByte(columnIndex: Int, x: Byte): Unit = underlying.updateByte(columnIndex + 1, x)

    def updateByte(columnLabel: String, x: Byte): Unit = underlying.updateByte(columnLabel, x)

    def updateBytes(columnIndex: Int, x: Array[Byte]): Unit = underlying.updateBytes(columnIndex + 1, x)

    def updateBytes(columnLabel: String, x: Array[Byte]): Unit = underlying.updateBytes(columnLabel, x)

    def updateDate(columnIndex: Int, x: java.sql.Date): Unit = underlying.updateDate(columnIndex + 1, x)

    def updateDate(columnLabel: String, x: java.sql.Date): Unit = underlying.updateDate(columnLabel, x)

    def updateDouble(columnIndex: Int, x: Double): Unit = underlying.updateDouble(columnIndex + 1, x)

    def updateDouble(columnLabel: String, x: Double): Unit = underlying.updateDouble(columnLabel, x)

    def updateFloat(columnIndex: Int, x: Float): Unit = underlying.updateFloat(columnIndex + 1, x)

    def updateFloat(columnLabel: String, x: Float): Unit = underlying.updateFloat(columnLabel, x)

    def updateInt(columnIndex: Int, x: Int): Unit = underlying.updateInt(columnIndex + 1, x)

    def updateInt(columnLabel: String, x: Int): Unit = underlying.updateInt(columnLabel, x)

    def updateLong(columnIndex: Int, x: Long): Unit = underlying.updateLong(columnIndex + 1, x)

    def updateLong(columnLabel: String, x: Long): Unit = underlying.updateLong(columnLabel, x)

    def updateNString(columnIndex: Int, nString: String): Unit = underlying.updateNString(columnIndex + 1, nString)

    def updateNString(columnLabel: String, nString: String): Unit = underlying.updateNString(columnLabel, nString)

    def updateRef(columnIndex: Int, x: java.sql.Ref): Unit = underlying.updateRef(columnIndex + 1, x)

    def updateRef(columnLabel: String, x: java.sql.Ref): Unit = underlying.updateRef(columnLabel, x)

    def updateRow(): Unit = underlying.updateRow()

    def rowUpdated(): Boolean = underlying.rowUpdated()

    def insertRow(): Unit = underlying.insertRow()

    def rowInserted(): Boolean = underlying.rowInserted()

    def deleteRow(): Unit = underlying.deleteRow()

    def rowDeleted(): Boolean = underlying.rowDeleted()

    def cancelRowUpdates(): Unit = underlying.cancelRowUpdates()

    def updateRowId(columnIndex: Int, x: RowId): Unit = underlying.updateRowId(columnIndex + 1, x)

    def updateRowId(columnLabel: String, x: RowId): Unit = underlying.updateRowId(columnLabel, x)

    def updateShort(columnIndex: Int, x: Short): Unit = underlying.updateShort(columnIndex + 1, x)

    def updateShort(columnLabel: String, x: Short): Unit = underlying.updateShort(columnLabel, x)

    def updateSQLXML(columnIndex: Int, xmlObject: SQLXML): Unit = underlying.updateSQLXML(columnIndex + 1, xmlObject)

    def updateSQLXML(columnLabel: String, xmlObject: SQLXML): Unit = underlying.updateSQLXML(columnLabel, xmlObject)

    def updateString(columnIndex: Int, x: String): Unit = underlying.updateString(columnIndex + 1, x)

    def updateString(columnLabel: String, x: String): Unit = underlying.updateString(columnLabel, x)

    def updateTime(columnIndex: Int, x: java.sql.Time): Unit = underlying.updateTime(columnIndex + 1, x)

    def updateTime(columnLabel: String, x: java.sql.Time): Unit = underlying.updateTime(columnLabel, x)

    def updateTimestamp(columnIndex: Int, x: java.sql.Timestamp): Unit = underlying.updateTimestamp(columnIndex + 1, x)

    def updateTimestamp(columnLabel: String, x: java.sql.Timestamp): Unit = underlying.updateTimestamp(columnLabel, x)

    def updateCharacterStream(columnIndex: Int, x: Reader, length: Int): Unit = underlying.updateCharacterStream(columnIndex + 1, x, length)

    def updateCharacterStream(columnLabel: String, x: Reader, length: Int): Unit = underlying.updateCharacterStream(columnLabel, x, length)

    def updateCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = underlying.updateCharacterStream(columnIndex + 1, x, length)

    def updateCharacterStream(columnLabel: String, x: Reader, length: Long): Unit = underlying.updateCharacterStream(columnLabel, x, length)

    def updateCharacterStream(columnIndex: Int, x: Reader): Unit = underlying.updateCharacterStream(columnIndex + 1, x)

    def updateCharacterStream(columnLabel: String, x: Reader): Unit = underlying.updateCharacterStream(columnLabel, x)

    def updateNCharacterStream(columnIndex: Int, x: Reader, length: Int): Unit = underlying.updateNCharacterStream(columnIndex + 1, x, length)

    def updateNCharacterStream(columnLabel: String, x: Reader, length: Int): Unit = underlying.updateNCharacterStream(columnLabel, x, length)

    def updateNCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = underlying.updateNCharacterStream(columnIndex + 1, x, length)

    def updateNCharacterStream(columnLabel: String, x: Reader, length: Long): Unit = underlying.updateNCharacterStream(columnLabel, x, length)

    def updateNCharacterStream(columnIndex: Int, x: Reader): Unit = underlying.updateNCharacterStream(columnIndex + 1, x)

    def updateNCharacterStream(columnLabel: String, x: Reader): Unit = underlying.updateNCharacterStream(columnLabel, x)

    def updateClob(columnIndex: Int, x: Clob, length: Int): Unit = underlying.updateClob(columnIndex + 1, x)

    def updateClob(columnLabel: String, x: Clob, length: Int): Unit = underlying.updateClob(columnLabel, x)

    def updateClob(columnIndex: Int, x: Reader, length: Long): Unit = underlying.updateClob(columnIndex + 1, x, length)

    def updateClob(columnLabel: String, x: Reader, length: Long): Unit = underlying.updateClob(columnLabel, x, length)

    def updateClob(columnIndex: Int, x: Reader): Unit = underlying.updateClob(columnIndex + 1, x)

    def updateClob(columnLabel: String, x: Reader): Unit = underlying.updateClob(columnLabel, x)

    def updateNClob(columnIndex: Int, x: NClob, length: Int): Unit = underlying.updateNClob(columnIndex + 1, x)

    def updateNClob(columnLabel: String, x: NClob, length: Int): Unit = underlying.updateNClob(columnLabel, x)

    def updateNClob(columnIndex: Int, x: Reader, length: Long): Unit = underlying.updateNClob(columnIndex + 1, x, length)

    def updateNClob(columnLabel: String, x: Reader, length: Long): Unit = underlying.updateNClob(columnLabel, x, length)

    def updateNClob(columnIndex: Int, x: Reader): Unit = underlying.updateNClob(columnIndex + 1, x)

    def updateNClob(columnLabel: String, x: Reader): Unit = underlying.updateNClob(columnLabel, x)

    def updateObject(columnIndex: Int, x: AnyRef): Unit = underlying.updateObject(columnIndex + 1, x)

    def updateObject(columnIndex: Int, x: AnyRef, scaleOrLength: Int): Unit = underlying.updateObject(columnIndex + 1, x, scaleOrLength)

    override def updateObject(columnIndex: Int, x: AnyRef, targetSqlType: SQLType): Unit = underlying.updateObject(columnIndex + 1, x, targetSqlType)

    override def updateObject(columnIndex: Int, x: AnyRef, targetSqlType: SQLType, scaleOrLength: Int): Unit = underlying.updateObject(columnIndex + 1, targetSqlType, scaleOrLength)

    override def updateObject(columnLabel: String, x: AnyRef): Unit = underlying.updateObject(columnLabel, x)

    override def updateObject(columnIndex: String, x: AnyRef, scaleOrLength: Int): Unit = underlying.updateObject(columnIndex + 1, x, scaleOrLength)

    override def updateObject(columnIndex: String, x: AnyRef, targetSqlType: SQLType): Unit = underlying.updateObject(columnIndex + 1, x, targetSqlType)

    override def updateObject(columnIndex: String, x: AnyRef, targetSqlType: SQLType, scaleOrLength: Int): Unit = underlying.updateObject(columnIndex + 1, targetSqlType, scaleOrLength)

    def updateNull(columnIndex: Int): Unit = underlying.updateNull(columnIndex + 1)

    def updateNull(columnLabel: String): Unit = underlying.updateNull(columnLabel)

    override def next(): Boolean =
      underlying.next()

    override def beforeFirst(): Unit =
      underlying.beforeFirst()

    override def updateNClob(columnIndex: Int, nClob: NClob): Unit =
      underlying.updateNClob(columnIndex, nClob)

    override def updateNClob(columnLabel: String, nClob: NClob): Unit =
      underlying.updateNClob(columnLabel, nClob)

    override def last(): Boolean =
      underlying.last()

    override def absolute(row: Int): Boolean =
      underlying.absolute(row + 1)

    override def moveToInsertRow(): Unit =
      underlying.moveToInsertRow()

    override def afterLast(): Unit =
      underlying.afterLast()

    override def setFetchDirection(direction: Int): Unit =
      underlying.setFetchDirection(direction)

    override def relative(rows: Int): Boolean =
      underlying.relative(rows)

    override def moveToCurrentRow(): Unit =
      underlying.moveToCurrentRow()

    override def updateClob(columnIndex: Int, x: Clob): Unit =
      underlying.updateClob(columnIndex, x)

    override def updateClob(columnLabel: String, x: Clob): Unit =
      underlying.updateClob(columnLabel, x)

    @Deprecated
    override def getBigDecimal(columnIndex: Int, scale: Int): BigDecimal =
      underlying.getBigDecimal(columnIndex, scale)

    @Deprecated
    override def getBigDecimal(columnLabel: String, scale: Int): BigDecimal =
      underlying.getBigDecimal(columnLabel, scale)

    @Deprecated
    override def getUnicodeStream(columnIndex: Int): InputStream =
      underlying.getUnicodeStream(columnIndex)

    @Deprecated
    override def getUnicodeStream(columnLabel: String): InputStream =
      underlying.getUnicodeStream(columnLabel)

    override def previous(): Boolean =
      underlying.previous()

    override def first(): Boolean =
      underlying.first()

    override def unwrap[T](iface: Class[T]): T =
      if (iface.isInstance(this)) iface.cast(this)
      else if (iface.isInstance(underlying)) iface.cast(underlying)
      else underlying.unwrap[T](iface)

    override def isWrapperFor(iface: Class[_]): Boolean =
      iface.isInstance(this) ||
        iface.isInstance(underlying) ||
        underlying.isWrapperFor(iface)
  }

  private[sdbc] object ConnectedRow {
    def apply(resultSet: ResultSet): ConnectedRow = {
      val columnNames = Row.columnNames(resultSet.getMetaData)
      val columnIndexes = Row.columnIndexes(columnNames)

      new ConnectedRow(
        underlying = resultSet,
        columnNames = columnNames,
        columnIndexes = columnIndexes
      )
    }

    def iterator(resultSet: ResultSet): CloseableIterator[ConnectedRow]  = {
      val columnNames = Row.columnNames(resultSet.getMetaData)
      val columnIndexes = Row.columnIndexes(columnNames)

      resultSet.iterator().map { resultSet =>
        new ConnectedRow(
          underlying = resultSet,
          columnNames = columnNames,
          columnIndexes = columnIndexes
        )
      }
    }

  }

  class UpdateableRow private[sdbc](
    underlying: ResultSet,
    columnNames: IndexedSeq[String],
    columnIndexes: Map[String, Int]
  ) extends ConnectedRow(underlying, columnNames, columnIndexes) {

    def update[T](columnIndex: Index, x: T)(implicit updater: Updater[T]): Unit = {
      updater.update(this, columnIndex(this), x)
    }

  }

  private[sdbc] object UpdateableRow {
    def apply(resultSet: ResultSet): UpdateableRow = {
      val columnNames = Row.columnNames(resultSet.getMetaData)
      val columnIndexes = Row.columnIndexes(columnNames)

      new UpdateableRow(
        underlying = resultSet,
        columnNames = columnNames,
        columnIndexes = columnIndexes
      )
    }

    def iterator(resultSet: ResultSet): CloseableIterator[UpdateableRow]  = {
      val columnNames = Row.columnNames(resultSet.getMetaData)
      val columnIndexes = Row.columnIndexes(columnNames)

      resultSet.iterator().map { resultSet =>
        new UpdateableRow(
          underlying = resultSet,
          columnNames = columnNames,
          columnIndexes = columnIndexes
        )
      }
    }

  }

}
