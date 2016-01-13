package com.rocketfuel.sdbc.cassandra.implementation

import java.net.InetAddress
import java.nio.ByteBuffer
import java.time.Instant
import java.util.UUID
import com.datastax.driver.core.DataType
import scodec.bits.ByteVector
import scala.collection.convert.decorateAsJava._

private[sdbc] trait TupleDataType {
  self: Cassandra =>

  case class TupleDataType[T](dataType: DataType, toCassandraValue: T => Any = (x: T) => x)

  object TupleDataType {

    implicit val int = TupleDataType[Int](DataType.cint())

    implicit val boxedInt = TupleDataType[java.lang.Integer](DataType.cint())

    implicit val long = TupleDataType[Long](DataType.bigint())

    implicit val boxedLong = TupleDataType[java.lang.Long](DataType.bigint())

    implicit val arrayByte = TupleDataType[Array[Byte]](DataType.blob())

    implicit val byteBuffer = TupleDataType[ByteBuffer](DataType.blob())

    implicit val byteVector = TupleDataType[ByteVector](DataType.blob())

    implicit val boolean = TupleDataType[Boolean](DataType.cboolean())

    implicit val boxedBoolean = TupleDataType[java.lang.Boolean](DataType.cboolean())

    implicit val double = TupleDataType[Double](DataType.cdouble())

    implicit val boxedDouble = TupleDataType[java.lang.Double](DataType.cdouble())

    implicit val float = TupleDataType[Float](DataType.cfloat())

    implicit val boxedFloat = TupleDataType[java.lang.Float](DataType.cfloat())

    implicit val inet = TupleDataType[InetAddress](DataType.inet())

    implicit val string = TupleDataType[String](DataType.text())

    implicit val uuid = TupleDataType[UUID](DataType.uuid())

    implicit val bigDecimal = TupleDataType[BigDecimal](DataType.decimal(), _.underlying())

    implicit val javaBigDecimal = TupleDataType[java.math.BigDecimal](DataType.decimal())

    implicit val date = TupleDataType[java.util.Date](DataType.timestamp())

    implicit val instant = TupleDataType[Instant](DataType.timestamp(), java.util.Date.from)

    implicit def seq[T](implicit innerType: TupleDataType[T]): TupleDataType[collection.immutable.Seq[T]] = {
      TupleDataType[collection.immutable.Seq[T]](DataType.list(innerType.dataType, true), _.asJava)
    }

    implicit def mutableSeq[T](implicit innerType: TupleDataType[T]): TupleDataType[collection.mutable.Seq[T]] = {
      TupleDataType[collection.mutable.Seq[T]](DataType.list(innerType.dataType, false), _.asJava)
    }

    implicit def immutableSet[T](implicit innerType: TupleDataType[T]): TupleDataType[collection.immutable.Set[T]] = {
      TupleDataType[collection.immutable.Set[T]](DataType.set(innerType.dataType, true), _.asJava)
    }

    implicit def mutableSet[T](implicit innerType: TupleDataType[T]): TupleDataType[collection.mutable.Set[T]] = {
      TupleDataType[collection.mutable.Set[T]](DataType.set(innerType.dataType, false), _.asJava)
    }

    implicit def immutableMap[K, V](implicit keyType: TupleDataType[K], valueType: TupleDataType[V]): TupleDataType[collection.immutable.Map[K, V]] = {
      TupleDataType[collection.immutable.Map[K, V]](DataType.map(keyType.dataType, valueType.dataType, true), _.asJava)
    }

    implicit def mutableMap[K, V](implicit keyType: TupleDataType[K], valueType: TupleDataType[V]): TupleDataType[collection.mutable.Map[K, V]] = {
      TupleDataType[collection.mutable.Map[K, V]](DataType.map(keyType.dataType, valueType.dataType, false), _.asJava)
    }

  }

}
