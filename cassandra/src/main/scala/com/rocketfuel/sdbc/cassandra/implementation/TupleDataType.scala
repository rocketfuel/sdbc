package com.rocketfuel.sdbc.cassandra.implementation

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.UUID

import com.datastax.driver.core.DataType
import scodec.bits.ByteVector

import scala.reflect.ClassTag

private[sdbc] case class TupleDataType[T](dataType: DataType)

private[sdbc] trait TupleDataTypes {

  implicit val intDataType = TupleDataType[Int](DataType.cint())

  implicit val boxedIntDataType = TupleDataType[java.lang.Integer](DataType.cint())

  implicit val longDataType = TupleDataType[Long](DataType.bigint())

  implicit val boxedLongDataType = TupleDataType[java.lang.Long](DataType.bigint())

  implicit val arrayByteDataType = TupleDataType[Array[Byte]](DataType.blob())

  implicit val byteBufferDataType = TupleDataType[ByteBuffer](DataType.blob())

  implicit val byteVectorDataType = TupleDataType[ByteVector](DataType.blob())

  implicit val booleanDataType = TupleDataType[Boolean](DataType.cboolean())

  implicit val boxedBooleanDataType = TupleDataType[java.lang.Boolean](DataType.cboolean())

  implicit val doubleDataType = TupleDataType[Double](DataType.cdouble())

  implicit val boxedDoubleDataType = TupleDataType[java.lang.Double](DataType.cdouble())

  implicit val floatDataType = TupleDataType[Float](DataType.cfloat())

  implicit val boxedFloatDataType = TupleDataType[java.lang.Float](DataType.cfloat())

  implicit val inetDataType = TupleDataType[InetAddress](DataType.inet())

  implicit val stringDataType = TupleDataType[String](DataType.text())

  implicit val uuidDataType = TupleDataType[UUID](DataType.uuid())

  implicit def immutableSeqDataType[T](implicit innerType: TupleDataType[T]): TupleDataType[collection.immutable.Seq[T]] = {
    TupleDataType[collection.immutable.Seq[T]](DataType.list(innerType.dataType, true))
  }

  implicit def mutableSeqDataType[T](implicit innerType: TupleDataType[T]): TupleDataType[collection.mutable.Seq[T]] = {
    TupleDataType[collection.mutable.Seq[T]](DataType.list(innerType.dataType, false))
  }

  implicit def immutableSetDataType[T](implicit innerType: TupleDataType[T]): TupleDataType[collection.immutable.Set[T]] = {
    TupleDataType[collection.immutable.Set[T]](DataType.set(innerType.dataType, true))
  }

  implicit def mutableSetDataType[T](implicit innerType: TupleDataType[T]): TupleDataType[collection.mutable.Set[T]] = {
    TupleDataType[collection.mutable.Set[T]](DataType.set(innerType.dataType, false))
  }

  implicit def immutableMapDataType[K, V](implicit keyType: TupleDataType[K], valueType: TupleDataType[V]): TupleDataType[collection.immutable.Map[K, V]] = {
    TupleDataType[collection.immutable.Map[K, V]](DataType.map(keyType.dataType, valueType.dataType, true))
  }

  implicit def mutableMapDataType[K, V](implicit keyType: TupleDataType[K], valueType: TupleDataType[V]): TupleDataType[collection.mutable.Map[K, V]] = {
    TupleDataType[collection.mutable.Map[K, V]](DataType.map(keyType.dataType, valueType.dataType, false))
  }

  def customDataType[T](implicit tag: ClassTag[T]): TupleDataType[T] = {
    TupleDataType[T](DataType.custom(tag.runtimeClass.getName))
  }

}
