package com.rocketfuel.sdbc.cassandra.implementation

import java.math.{BigInteger, BigDecimal}
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util
import java.util.{UUID, Date}

import com.datastax.driver.core
import com.datastax.driver.core.ColumnDefinitions
import com.google.common.reflect.TypeToken
import com.rocketfuel.sdbc.base

trait Row extends base.Index {
  self: Cassandra =>

  override def getColumnCount(row: Row): Int = row.getColumnDefinitions.size()

  override def getColumnIndex(row: Row, columnName: String): Int = {
    row.getColumnDefinitions.getIndexOf(columnName) match {
      case -1 => throw new NoSuchElementException("key not found: " + columnName)
      case columnIndex => columnIndex
    }
  }

  override def containsColumn(row: Row, columnName: String): Boolean = {
    row.getColumnDefinitions.contains(columnName)
  }

  case class Row(underlying: core.Row) extends core.Row {

    def apply[T](ix: Index)(implicit getter: CompositeGetter[T]): T = {
      getter(this, ix(this))
    }

    override def getUUID(i: Int): UUID = underlying.getUUID(i: Int)

    override def getUUID(name: String): UUID = underlying.getUUID(name: String)

    override def getVarint(i: Int): BigInteger = underlying.getVarint(i: Int)

    override def getVarint(name: String): BigInteger = underlying.getVarint(name: String)

    override def getInet(i: Int): InetAddress = underlying.getInet(i: Int)

    override def getInet(name: String): InetAddress = underlying.getInet(name: String)

    override def getList[T](i: Int, elementsClass: Class[T]): util.List[T] = underlying.getList[T](i: Int, elementsClass: Class[T])

    override def getList[T](name: String, elementsClass: Class[T]): util.List[T] = underlying.getList[T](name: String, elementsClass: Class[T])

    override def getDouble(i: Int): Double = underlying.getDouble(i: Int)

    override def getDouble(name: String): Double = underlying.getDouble(name: String)

    override def getColumnDefinitions: ColumnDefinitions = underlying.getColumnDefinitions

    override def getBytesUnsafe(i: Int): ByteBuffer = underlying.getBytesUnsafe(i: Int)

    override def getBytesUnsafe(name: String): ByteBuffer = underlying.getBytesUnsafe(name: String)

    override def getFloat(i: Int): Float = underlying.getFloat(i: Int)

    override def getFloat(name: String): Float = underlying.getFloat(name: String)

    override def getLong(i: Int): Long = underlying.getLong(i: Int)

    override def getLong(name: String): Long = underlying.getLong(name: String)

    override def getBool(i: Int): Boolean = underlying.getBool(i: Int)

    override def getBool(name: String): Boolean = underlying.getBool(name: String)

    override def getMap[K, V](i: Int, keysClass: Class[K], valuesClass: Class[V]): util.Map[K, V] = underlying.getMap[K, V](i: Int, keysClass: Class[K], valuesClass: Class[V])

    override def getMap[K, V](name: String, keysClass: Class[K], valuesClass: Class[V]): util.Map[K, V] = underlying.getMap[K, V](name: String, keysClass: Class[K], valuesClass: Class[V])

    override def getToken(i: Int): Token = underlying.getToken(i: Int)

    override def getToken(name: String): Token = underlying.getToken(name: String)

    override def getPartitionKeyToken: Token = underlying.getPartitionKeyToken

    override def getDecimal(i: Int): BigDecimal = underlying.getDecimal(i: Int)

    override def getDecimal(name: String): BigDecimal = underlying.getDecimal(name: String)

    override def isNull(i: Int): Boolean = underlying.isNull(i: Int)

    override def isNull(name: String): Boolean = underlying.isNull(name: String)

    override def getSet[T](i: Int, elementsClass: Class[T]): util.Set[T] = underlying.getSet[T](i: Int, elementsClass: Class[T])

    override def getSet[T](name: String, elementsClass: Class[T]): util.Set[T] = underlying.getSet[T](name: String, elementsClass: Class[T])

    override def getDate(i: Int): Date = underlying.getDate(i: Int)

    override def getDate(name: String): Date = underlying.getDate(name: String)

    override def getInt(i: Int): Int = underlying.getInt(i: Int)

    override def getInt(name: String): Int = underlying.getInt(name: String)

    override def getBytes(i: Int): ByteBuffer = underlying.getBytes(i: Int)

    override def getBytes(name: String): ByteBuffer = underlying.getBytes(name: String)

    override def getString(i: Int): String = underlying.getString(i: Int)

    override def getString(name: String): String = underlying.getString(name: String)

    override def getTupleValue(i: Int): core.TupleValue = underlying.getTupleValue(i: Int)

    override def getList[T](i: Int, elementsType: TypeToken[T]): util.List[T] = underlying.getList[T](i: Int, elementsType: TypeToken[T])

    override def getUDTValue(i: Int): UDTValue = underlying.getUDTValue(i: Int)

    override def getMap[K, V](i: Int, keysType: TypeToken[K], valuesType: TypeToken[V]): util.Map[K, V] = underlying.getMap[K, V](i: Int, keysType: TypeToken[K], valuesType: TypeToken[V])

    override def getObject(i: Int): AnyRef = underlying.getObject(i: Int)

    override def getSet[T](i: Int, elementsType: TypeToken[T]): util.Set[T] = underlying.getSet[T](i: Int, elementsType: TypeToken[T])

    override def getTupleValue(name: String): core.TupleValue = underlying.getTupleValue(name: String)

    override def getList[T](name: String, elementsType: TypeToken[T]): util.List[T] = underlying.getList[T](name: String, elementsType: TypeToken[T])

    override def getUDTValue(name: String): UDTValue = underlying.getUDTValue(name: String)

    override def getMap[K, V](name: String, keysType: TypeToken[K], valuesType: TypeToken[V]): util.Map[K, V] = underlying.getMap[K, V](name: String, keysType: TypeToken[K], valuesType: TypeToken[V])

    override def getObject(name: String): AnyRef = underlying.getObject(name: String)

    override def getSet[T](name: String, elementsType: TypeToken[T]): util.Set[T] = underlying.getSet[T](name: String, elementsType: TypeToken[T])
  }

}
