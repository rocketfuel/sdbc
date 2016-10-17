package com.rocketfuel.sdbc.base.jdbc.resultset

import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.jdbc.DBMS
import java.math.BigDecimal
import java.net.URL
import java.sql.{Array => _, _}

trait ImmutableRow {
  self: DBMS =>

  /**
    * A data type that is like a JDBC ResultSet, but only the parts that are safe to use after that connection is
    * closed.
    *
    * You can construct an ImmutableRow by calling the UpdatableRow's asImmutable method.
    *
    * @param columnNames
    * @param getMetaData
    * @param getRow
    */
  class ImmutableRow private[sdbc](
    override val columnNames: IndexedSeq[String],
    override val columnIndexes: Map[String, Int],
    override val getMetaData: ResultSetMetaData,
    override val getRow: Int,
    override val toSeq: IndexedSeq[Option[Any]]
  ) extends Row() {

    override lazy val toMap = Row.toMap(toSeq, getMetaData)

    private var _wasNull = false

    override def wasNull: Boolean = _wasNull

    private def setWasNull[T](columnIndex: Int, typeName: String)(get: PartialFunction[Any, T]): Option[T] = {
      val parameter = toSeq(columnIndex)
      _wasNull = parameter.isEmpty
      parameter.map(get.orElse {case _ => ImmutableRow.incorrectType(typeName) })
    }

    override def getString(columnIndex: Int): String =
      setWasNull[String](columnIndex, "text") {
        case s: String => s
      } orNull

    override def getString(columnLabel: String): String =
      getString(columnIndexes(columnLabel))

    override def getLong(columnIndex: Int): Long =
      setWasNull[Long](columnIndex, "bigint") {
        case s: Long => s
        case s: String => s.toLong
      } getOrElse 0L

    override def getLong(columnLabel: String): Long = getLong(columnIndexes(columnLabel))

    override def getTimestamp(columnIndex: Int): Timestamp =
      setWasNull(columnIndex, "timestamp") {
        case s: Timestamp => s
      } orNull

    override def getTimestamp(columnLabel: String): Timestamp = getTimestamp(columnIndexes(columnLabel))

    override def getDouble(columnIndex: Int): Double = {
      setWasNull(columnIndex, "float8") {
        case s: Double => s
      } getOrElse 0.0
    }

    override def getDouble(columnLabel: String): Double = getDouble(columnIndexes(columnLabel))

    override def getURL(columnIndex: Int): URL =
      setWasNull(columnIndex, "url") {
        case s: URL => s
        case s: String => new URL(s)
      } orNull

    override def getURL(columnLabel: String): URL = getURL(columnIndexes(columnLabel))

    override def getBigDecimal(columnIndex: Int): BigDecimal =
      setWasNull(columnIndex, "numeric") {
        case s: BigDecimal => s
        case s: String => new BigDecimal(s)
      } orNull

    override def getBigDecimal(columnLabel: String): BigDecimal = getBigDecimal(columnIndexes(columnLabel))

    override def getFloat(columnIndex: Int): Float =
      setWasNull(columnIndex, "float4") {
        case s: Float => s
        case s: String => s.toFloat
      } getOrElse 0F

    override def getFloat(columnLabel: String): Float = getFloat(columnIndexes(columnLabel))

    override def getTime(columnIndex: Int): Time =
      setWasNull(columnIndex, "time") {
        case s: Time => s
      } orNull

    override def getTime(columnLabel: String): Time = getTime(columnIndexes(columnLabel))

    override def getByte(columnIndex: Int): Byte =
      setWasNull(columnIndex, "int1") {
        case s: Byte => s
        case s: String => s.toByte
      } getOrElse 0.toByte

    override def getByte(columnLabel: String): Byte = getByte(columnIndexes(columnLabel))

    override def getBoolean(columnIndex: Int): Boolean =
      setWasNull(columnIndex, "bool") {
        case s: Boolean => s
      } exists identity

    override def getBoolean(columnLabel: String): Boolean = getBoolean(columnIndexes(columnLabel))

    override def getShort(columnIndex: Int): Short =
      setWasNull(columnIndex, "int2") {
        case s: Short => s
        case s: String => s.toShort
      } getOrElse 0.toShort

    override def getShort(columnLabel: String): Short = getShort(columnIndexes(columnLabel))

    override def getDate(columnIndex: Int): Date =
      setWasNull(columnIndex, "date") { case s: Date => s } orNull

    override def getDate(columnLabel: String): Date = getDate(columnIndexes(columnLabel))

    override def getSQLXML(columnIndex: Int): SQLXML =
      setWasNull(columnIndex, "xml") { case s: SQLXML => s } orNull

    override def getSQLXML(columnLabel: String): SQLXML = getSQLXML(columnIndexes(columnLabel))

    override def getInt(columnIndex: Int): Int =
      setWasNull(columnIndex, "int4") {
        case s: Int => s
        case s: String => s.toInt
      } getOrElse 0

    override def getInt(columnLabel: String): Int = getInt(columnIndexes(columnLabel))

    override def getBytes(columnIndex: Int): Array[Byte] =
      setWasNull(columnIndex, "bytea") {
        case s: Array[Byte] => s
      } orNull

    override def getBytes(columnLabel: String): Array[Byte] = getBytes(columnIndexes(columnLabel))

    override def getObject(columnIndex: Int): AnyRef =
      setWasNull[AnyRef](columnIndex, "any") {
        case x => base.box(x)
      } orNull

    override def getObject(columnLabel: String): AnyRef =
      getObject(columnIndexes(columnLabel))

  }

  object ImmutableRow {

    def apply(resultSet: ResultSet): ImmutableRow = {
      val getMetadata = resultSet.getMetaData
      val columnNames = Row.columnNames(getMetadata)
      val columnIndexes = Row.columnIndexes(columnNames)
      val getRow = resultSet.getRow - 1
      val toSeq = Row.toSeq(resultSet)

      new ImmutableRow(
        columnNames = columnNames,
        columnIndexes = columnIndexes,
        getMetaData = getMetadata,
        getRow = getRow,
        toSeq = toSeq
      )
    }

    def iterator(resultSet: ResultSet): CloseableIterator[ImmutableRow] = {
      val getMetadata = resultSet.getMetaData
      val columnNames = Row.columnNames(getMetadata)
      val columnIndexes = Row.columnIndexes(columnNames)

      resultSet.iterator().mapCloseable { resultSet =>
        val getRow = resultSet.getRow - 1
        val toSeq = Row.toSeq(resultSet)

        new ImmutableRow(
          columnNames = columnNames,
          columnIndexes = columnIndexes,
          getMetaData = getMetadata,
          getRow = getRow,
          toSeq = toSeq
        )
      }
    }

    private def incorrectType(typeName: String): Nothing = {
      throw new SQLException("column does not contain a " + typeName)
    }

  }

}
