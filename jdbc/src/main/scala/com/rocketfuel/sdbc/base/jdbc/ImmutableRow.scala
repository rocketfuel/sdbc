package com.rocketfuel.sdbc.base.jdbc

import java.io.{InputStream, Reader}
import java.math.BigDecimal
import java.net.URL
import java.sql.{Array => JdbcArray, _}
import com.rocketfuel.sdbc.base

trait ImmutableRow {
  self: ParameterValue
    with Row
    with Getter
    with CompositeGetter
    with Row
    with MutableRow =>

  case class ImmutableRow private[sdbc](
    override val columnTypes: IndexedSeq[String],
    override val columnNames: IndexedSeq[String],
    override val asIntMap: IndexedSeq[Option[Any]],
    override val getMetaData: ResultSetMetaData,
    override val getRow: Int
  ) extends Row {

    private var _wasNull = false

    override def wasNull: Boolean = _wasNull

    private def setWasNull(columnIndex: Int): Option[Any] = {
      val parameter = asIntMap(columnIndex)
      _wasNull = parameter.isEmpty
      parameter
    }

    override def getString(columnIndex: Int): String = {
      setWasNull(columnIndex) map { case s: String => s case _ => ImmutableRow.incorrectType("text") } orNull
    }

    override def getString(columnLabel: String): String = {
      getString(columnIndexes(columnLabel))
    }

    override def getLong(columnIndex: Int): Long = {
      setWasNull(columnIndex) map { case s: Long => s case _ => ImmutableRow.incorrectType("bigint") } getOrElse 0L
    }

    override def getLong(columnLabel: String): Long = getLong(columnIndexes(columnLabel))

    override def getTimestamp(columnIndex: Int): Timestamp = {
      setWasNull(columnIndex) map { case s: Timestamp => s case _ => ImmutableRow.incorrectType("timestamp") } orNull
    }

    override def getTimestamp(columnLabel: String): Timestamp = getTimestamp(columnIndexes(columnLabel))

    override def getBinaryStream(columnIndex: Int): InputStream = {
      setWasNull(columnIndex) map { case s: InputStream => s case _ => ImmutableRow.incorrectType("bytea") } orNull
    }

    override def getBinaryStream(columnLabel: String): InputStream = getBinaryStream(columnIndexes(columnLabel))

    override def getDouble(columnIndex: Int): Double = {
      setWasNull(columnIndex) map { case s: Double => s case _ => ImmutableRow.incorrectType("float8") } getOrElse 0.0
    }

    override def getDouble(columnLabel: String): Double = getDouble(columnIndexes(columnLabel))

    override def getCharacterStream(columnIndex: Int): Reader = {
      setWasNull(columnIndex) map { case s: Reader => s case _ => ImmutableRow.incorrectType("text") } orNull
    }

    override def getCharacterStream(columnLabel: String): Reader = getCharacterStream(columnIndexes(columnLabel))

    override def getArray(columnIndex: Int): JdbcArray = {
      setWasNull(columnIndex) map { case s: JdbcArray => s case _ => ImmutableRow.incorrectType("array") } orNull
    }

    override def getArray(columnLabel: String): JdbcArray = getArray(columnIndexes(columnLabel))

    override def getURL(columnIndex: Int): URL = {
      setWasNull(columnIndex) map { case s: URL => s case _ => ImmutableRow.incorrectType("url") } orNull
    }

    override def getURL(columnLabel: String): URL = getURL(columnIndexes(columnLabel))

    override def getBigDecimal(columnIndex: Int): BigDecimal = {
      setWasNull(columnIndex) map { case s: BigDecimal => s case _ => ImmutableRow.incorrectType("numeric") } orNull
    }

    override def getBigDecimal(columnLabel: String): BigDecimal = getBigDecimal(columnIndexes(columnLabel))

    override def getFloat(columnIndex: Int): Float = {
      setWasNull(columnIndex) map { case s: Float => s case _ => ImmutableRow.incorrectType("float4") } getOrElse 0F
    }

    override def getFloat(columnLabel: String): Float = getFloat(columnIndexes(columnLabel))

    override def getTime(columnIndex: Int): Time = {
      setWasNull(columnIndex) map { case s: Time => s case _ => ImmutableRow.incorrectType("time") } orNull
    }

    override def getTime(columnLabel: String): Time = getTime(columnIndexes(columnLabel))

    override def getByte(columnIndex: Int): Byte = {
      setWasNull(columnIndex) map { case s: Byte => s case _ => ImmutableRow.incorrectType("int1") } getOrElse 0
    }

    override def getByte(columnLabel: String): Byte = getByte(columnIndexes(columnLabel))

    override def getBoolean(columnIndex: Int): Boolean = {
      setWasNull(columnIndex) map { case s: Boolean => s case _ => ImmutableRow.incorrectType("bool") } exists identity
    }

    override def getBoolean(columnLabel: String): Boolean = getBoolean(columnIndexes(columnLabel))

    override def getShort(columnIndex: Int): Short = {
      setWasNull(columnIndex) map { case s: Short => s case _ => ImmutableRow.incorrectType("int2") } getOrElse 0
    }

    override def getShort(columnLabel: String): Short = getShort(columnIndexes(columnLabel))

    override def getDate(columnIndex: Int): Date = {
      setWasNull(columnIndex) map { case s: Date => s case _ => ImmutableRow.incorrectType("date") } orNull
    }

    override def getDate(columnLabel: String): Date = getDate(columnIndexes(columnLabel))

    override def getSQLXML(columnIndex: Int): SQLXML = {
      setWasNull(columnIndex) map { case s: SQLXML => s case _ => ImmutableRow.incorrectType("xml") } orNull
    }

    override def getSQLXML(columnLabel: String): SQLXML = getSQLXML(columnIndexes(columnLabel))

    override def getInt(columnIndex: Int): Int = {
      setWasNull(columnIndex) map { case s: Int => s case _ => ImmutableRow.incorrectType("int4") } getOrElse (0)
    }

    override def getInt(columnLabel: String): Int = getInt(columnIndexes(columnLabel))

    override def getBlob(columnIndex: Int): Blob = {
      setWasNull(columnIndex) map { case s: Blob => s case _ => ImmutableRow.incorrectType("bytea") } orNull
    }

    override def getBlob(columnLabel: String): Blob = getBlob(columnIndexes(columnLabel))

    override def getBytes(columnIndex: Int): Array[Byte] = {
      setWasNull(columnIndex) map { case s: Array[Byte] => s case _ => ImmutableRow.incorrectType("bytea") } orNull
    }

    override def getBytes(columnLabel: String): Array[Byte] = getBytes(columnIndexes(columnLabel))

    override def getObject(columnIndex: Int): AnyRef = {
      setWasNull(columnIndex) map base.box orNull
    }

    override def getObject(columnLabel: String): AnyRef = {
      getObject(columnIndexes(columnLabel))
    }

    override def getClob(columnIndex: Int): Clob = {
      throw new UnsupportedOperationException()
    }

    override def getClob(columnLabel: String): Clob = {
      getClob(columnIndexes(columnLabel))
    }
  }

  object ImmutableRow {
    implicit def apply(row: Row): ImmutableRow = {
      ImmutableRow(
        columnTypes = row.columnTypes,
        columnNames = row.columnNames,
        asIntMap = row.asIntMap,
        getMetaData = row.getMetaData,
        getRow = row.getRow
      )
    }

    private def incorrectType(typeName: String): Nothing = {
      throw new SQLException("column does not contain a " + typeName)
    }
  }

}
