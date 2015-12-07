package com.rocketfuel.sdbc.base.jdbc

import java.io.{Reader, InputStream}
import java.math.BigDecimal
import java.net.URL
import java.sql.{Array => JdbcArray, _}
import com.rocketfuel.sdbc.base.CIMap

trait Row {
  self: Getter
    with CompositeGetter
    with MutableRow
    with Index
    with ParameterValue =>

  abstract class Row {

    //TODO: Use ProductTypeclass from shapeless to get HList support, and therefore also tuples and case classes
    //Tip from tpolecat

    def columnTypes: IndexedSeq[String]

    def columnNames: IndexedSeq[String]

    lazy val columnIndexes = CIMap(columnNames.zipWithIndex: _*)

    def asStringMap: Map[String, Option[Any]] = {
      asIntMap.zipWithIndex.map {
        case (value, ix) =>
          val columnName = columnNames(ix)
          columnName -> value
      }.toMap
    }

    def asIntMap: IndexedSeq[Option[Any]]

    /**
      * The index of the row in the result set.
      * @return
      */
    def getRow: Int

    def getMetaData: ResultSetMetaData

    def get[T](columnIndex: Index.Index)(implicit getter: Getter[T]): T = {
      getter(this, columnIndex)
    }

    def wasNull: Boolean

    def getTimestamp(columnIndex: Int): Timestamp

    def getTimestamp(columnLabel: String): Timestamp

    def getBinaryStream(columnIndex: Int): InputStream

    def getBinaryStream(columnLabel: String): InputStream

    def getCharacterStream(columnIndex: Int): Reader

    def getCharacterStream(columnLabel: String): Reader

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

    def getBlob(columnIndex: Int): Blob

    def getBlob(columnLabel: String): Blob

    def getBytes(columnIndex: Int): Array[Byte]

    def getBytes(columnLabel: String): Array[Byte]

    def getString(columnIndex: Int): String

    def getString(columnLabel: String): String

    def getObject(columnIndex: Int): AnyRef

    def getObject(columnLabel: String): AnyRef

    def getClob(columnIndex: Int): Clob

    def getClob(columnLabel: String): Clob
  }

}
