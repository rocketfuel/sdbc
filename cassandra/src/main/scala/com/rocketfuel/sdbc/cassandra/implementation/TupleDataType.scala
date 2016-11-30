package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core
import com.datastax.driver.core.DataType
import java.net.InetAddress
import java.nio.ByteBuffer
import java.time.Instant
import java.util.UUID
import scala.collection.JavaConverters._
import scodec.bits.ByteVector

private[sdbc] trait TupleDataType {
  self: Cassandra =>

  trait TupleDataType[A] {
    type CassandraType <: AnyRef

    val dataType: DataType

    def toCassandraValue(value: A): CassandraType
  }

  object TupleDataType {

    type Aux[A, CassandraType0 <: AnyRef] = TupleDataType[A] { type CassandraType = CassandraType0 }

    def apply[A](implicit tupleDataType: TupleDataType[A]): TupleDataType[A] = tupleDataType

    def ofIdentity[A <: AnyRef](dataType: DataType): TupleDataType[A] = {
      ofConvertable[A, A](dataType)(identity)
    }

    def ofConvertable[A, CassandraType0 <: AnyRef](dataType0: DataType)(implicit conversion: A => CassandraType0): TupleDataType[A] =
      new TupleDataType[A] {
        override type CassandraType = CassandraType0

        override val dataType = dataType0

        override def toCassandraValue(value: A): CassandraType = conversion(value)
      }

    implicit val int = ofConvertable[Int, java.lang.Integer](DataType.cint())

    implicit val boxedInt = ofIdentity[java.lang.Integer](DataType.cint())

    implicit val long = ofConvertable[Long, java.lang.Long](DataType.bigint())

    implicit val boxedLong = ofIdentity[java.lang.Long](DataType.bigint())

    implicit val arrayByte = ofConvertable[Array[Byte], Array[Byte]](DataType.blob())(_.clone())

    implicit val byteBuffer = ofConvertable[ByteBuffer, Array[Byte]](DataType.blob())(_.array().clone())

    implicit val byteVector = ofConvertable[ByteVector, Array[Byte]](DataType.blob())(_.toArray)

    implicit val boolean = ofConvertable[Boolean, java.lang.Boolean](DataType.cboolean())

    implicit val boxedBoolean = ofIdentity[java.lang.Boolean](DataType.cboolean())

    implicit val double = ofConvertable[Double, java.lang.Double](DataType.cdouble())

    implicit val boxedDouble = ofIdentity[java.lang.Double](DataType.cdouble())

    implicit val float = ofConvertable[Float, java.lang.Float](DataType.cfloat())

    implicit val boxedFloat = ofIdentity[java.lang.Float](DataType.cfloat())

    implicit val inet = ofIdentity[InetAddress](DataType.inet())

    implicit val string = ofIdentity[String](DataType.text())

    implicit val uuid = ofIdentity[UUID](DataType.uuid())

    implicit val bigDecimal = ofConvertable[BigDecimal, java.math.BigDecimal](DataType.decimal())(_.underlying())

    implicit val javaBigDecimal = ofIdentity[java.math.BigDecimal](DataType.decimal())

    implicit val date = ofIdentity[java.util.Date](DataType.timestamp())

    implicit val instant = ofConvertable[Instant, java.util.Date](DataType.timestamp())(java.util.Date.from)

    implicit val none: TupleDataType[None.type] =
      ofConvertable(core.DataType.custom(classOf[java.lang.Object].getName))(Function.const(null))

    implicit def option[A](implicit innerType: TupleDataType[A]): TupleDataType[Option[A]] = {
      ofConvertable[
        Option[A],
        innerType.CassandraType
      ](innerType.dataType)(
        _.map(value => innerType.toCassandraValue(value)).getOrElse(null.asInstanceOf[innerType.CassandraType])
      )
    }

    implicit def seq[A](implicit innerType: TupleDataType[A]): TupleDataType[collection.immutable.Seq[A]] = {
      ofConvertable[
        collection.immutable.Seq[A],
        java.util.List[innerType.CassandraType]
      ](DataType.list(innerType.dataType, true)
      )(_.map(value => innerType.toCassandraValue(value)).asJava
      )
    }

    implicit def mutableSeq[A](implicit innerType: TupleDataType[A]): TupleDataType[collection.mutable.Seq[A]] = {
      ofConvertable[
        collection.mutable.Seq[A],
        java.util.List[innerType.CassandraType]
      ](DataType.list(innerType.dataType, false)
      )(_.map(value => innerType.toCassandraValue(value)).asJava
      )
    }

    implicit def set[A](implicit innerType: TupleDataType[A]): TupleDataType[collection.immutable.Set[A]] = {
      ofConvertable[
        collection.immutable.Set[A],
        java.util.Set[innerType.CassandraType]
      ](DataType.set(innerType.dataType, true)
      )(_.map(value => innerType.toCassandraValue(value)).asJava
      )
    }

    implicit def mutableSet[A](implicit innerType: TupleDataType[A]): TupleDataType[collection.mutable.Set[A]] = {
      ofConvertable[
        collection.mutable.Set[A],
        java.util.Set[innerType.CassandraType]
      ](DataType.set(innerType.dataType, false)
      )(_.map(value => innerType.toCassandraValue(value)).asJava
      )
    }

    implicit def immutableMap[K, V](implicit keyType: TupleDataType[K], valueType: TupleDataType[V]): TupleDataType[collection.immutable.Map[K, V]] = {
      ofConvertable[
        collection.immutable.Map[K, V],
        java.util.Map[keyType.CassandraType, valueType.CassandraType]
      ](DataType.map(keyType.dataType, valueType.dataType, true)
      )(_.map {case (k, v) => (keyType.toCassandraValue(k), valueType.toCassandraValue(v))}.asJava
      )
    }

    implicit def mutableMap[K, V](implicit keyType: TupleDataType[K], valueType: TupleDataType[V]): TupleDataType[collection.mutable.Map[K, V]] = {
      ofConvertable[
        collection.mutable.Map[K, V],
        java.util.Map[keyType.CassandraType, valueType.CassandraType]
      ](DataType.map(keyType.dataType, valueType.dataType, false)
      )(_.map {case (k, v) => (keyType.toCassandraValue(k), valueType.toCassandraValue(v))}.asJava
      )
    }

  }

}
