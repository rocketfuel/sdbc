package com.rocketfuel.sdbc.cassandra

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes}
import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.data.GettableByIndex
import java.math.{BigDecimal, BigInteger}
import java.net.InetAddress
import java.nio.ByteBuffer
import java.time.{Instant, LocalDate, LocalTime}
import java.util
import java.util.UUID
import shapeless.HList
import shapeless.ops.hlist.{Mapper, ToTraversable}
import shapeless.ops.product.ToHList

case class TupleValue(underlying: com.datastax.oss.driver.api.core.data.TupleValue) extends GettableByIndex {

  def apply[A](implicit compositeTupleGetter: CompositeTupleGetter[A]): A = {
    compositeTupleGetter(this, 0)
  }

  override def getUuid(i: Int): UUID = underlying.getUuid(i: Int)

  override def getBigInteger(i: Int): BigInteger = underlying.getBigInteger(i: Int)

  override def getInetAddress(i: Int): InetAddress = underlying.getInetAddress(i: Int)

  override def getList[T](i: Int, elementsClass: Class[T]): util.List[T] = underlying.getList[T](i: Int, elementsClass: Class[T])

  override def getDouble(i: Int): Double = underlying.getDouble(i: Int)

  override def getBytesUnsafe(i: Int): ByteBuffer = underlying.getBytesUnsafe(i: Int)

  override def getFloat(i: Int): Float = underlying.getFloat(i: Int)

  override def getLong(i: Int): Long = underlying.getLong(i: Int)

  override def getBoolean(i: Int): Boolean = underlying.getBoolean(i: Int)

  override def getMap[K, V](i: Int, keysClass: Class[K], valuesClass: Class[V]): util.Map[K, V] = underlying.getMap[K, V](i: Int, keysClass: Class[K], valuesClass: Class[V])

  override def getBigDecimal(i: Int): BigDecimal = underlying.getBigDecimal(i: Int)

  override def isNull(i: Int): Boolean = underlying.isNull(i: Int)

  override def getSet[T](i: Int, elementsClass: Class[T]): util.Set[T] = underlying.getSet[T](i: Int, elementsClass: Class[T])

  override def getLocalDate(i: Int): LocalDate = underlying.getLocalDate(i: Int)

  override def getInt(i: Int): Int = underlying.getInt(i: Int)

  override def getByteBuffer(i: Int): ByteBuffer = underlying.getByteBuffer(i: Int)

  override def getString(i: Int): String = underlying.getString(i: Int)

  override def getTupleValue(i: Int): com.datastax.oss.driver.api.core.data.TupleValue = underlying.getTupleValue(i: Int)

  override def getObject(i: Int): AnyRef = underlying.getObject(i: Int)

  override def getInstant(i: Int): Instant = underlying.getInstant(i)

  override def get[T](i: Int, targetClass: Class[T]): T = underlying.get(i, targetClass)

  override def getLocalTime(i: Int): LocalTime = underlying.getLocalTime(i)

  override def getByte(i: Int): Byte = underlying.getByte(i)

  override def getShort(i: Int): Short = underlying.getShort(i)

  override def size(): Int = underlying.size()

  override def getType(i: Int): DataType = underlying.getType(i)

  override def codecRegistry(): CodecRegistry = underlying.codecRegistry()

  override def protocolVersion(): ProtocolVersion = underlying.protocolVersion()
}

object TupleValue {
  implicit def of(underlying: com.datastax.oss.driver.api.core.data.TupleValue): TupleValue = {
    apply(underlying)
  }

  implicit def hlistTupleValue[
    H <: HList,
    ListH <: HList,
    MappedTypesH <: HList,
    MappedValuesH <: HList
  ](h: H
  )(implicit dataTypeMapper: Mapper.Aux[TupleDataType.ToDataType.type, H, MappedTypesH],
    dataTypeList: ToTraversable.Aux[MappedTypesH, Seq, DataType],
    dataValueMapper: Mapper.Aux[TupleDataType.ToDataValue.type, H, MappedValuesH],
    dataValueList: ToTraversable.Aux[MappedValuesH, Seq, AnyRef]
  ): TupleValue = {
    val dataTypes = dataTypeList(h.map(TupleDataType.ToDataType))
    val dataValueHList = h.map(TupleDataType.ToDataValue)
    val dataValues = dataValueList(dataValueHList)
    val underlying = DataTypes.tupleOf(dataTypes: _*)
                     .newValue(dataValues: _*)
    TupleValue(underlying)
  }

  implicit def productTupleValue[
    P,
    H <: HList,
    ListH <: HList,
    MappedTypesH <: HList,
    MappedValuesH <: HList
  ](p: P
  )(implicit toHList: ToHList.Aux[P, H],
    dataTypeMapper: Mapper.Aux[TupleDataType.ToDataType.type, H, MappedTypesH],
    dataTypeList: ToTraversable.Aux[MappedTypesH, Seq, DataType],
    dataValueMapper: Mapper.Aux[TupleDataType.ToDataValue.type, H, MappedValuesH],
    dataValueList: ToTraversable.Aux[MappedValuesH, Seq, AnyRef]
  ): TupleValue = {
    val asH = toHList(p)
    val tv = hlistTupleValue(asH)
    tv
  }
}
