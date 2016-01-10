package com.rocketfuel.sdbc.cassandra.implementation

import java.net.InetAddress
import java.nio.ByteBuffer
import java.time.Instant
import java.util.{Date, UUID}
import com.google.common.reflect.TypeToken
import com.rocketfuel.sdbc.base
import scodec.bits.ByteVector
import scala.collection.convert.wrapAsScala._

trait TupleGetter {
  self: Cassandra =>

  private[sdbc] trait TupleGetter[+T] extends base.Getter[TupleValue, Int, T]
  
  object TupleGetter {
    implicit def of[T](getter: TupleValue => Int => T): TupleGetter[T] = {
      new TupleGetter[T] {
        override def apply(tuple: TupleValue, ix: Int): Option[T] = {
          
          if (tuple.isNull(ix)) None
          else Some(getter(tuple)(ix))
        }
      }
    }

    implicit def ofTupleDataType[T](implicit tupleType: TupleDataType[T]): TupleGetter[T] = {
      new TupleGetter[T] {
        override def apply(v1: TupleValue, v2: Int): Option[T] = {
          Option(v1.getObject(v2)).map(_.asInstanceOf[T])
        }
      }
    }

    implicit val BooleanTupleGetter: TupleGetter[Boolean] = of[Boolean](tuple => ix => tuple.getBool(ix))

    implicit val BoxedBooleanTupleGetter: TupleGetter[java.lang.Boolean] = of[java.lang.Boolean](tuple => ix => tuple.getBool(ix))

    implicit val ByteVectorTupleGetter: TupleGetter[ByteVector] = of[ByteVector](tuple => ix => ByteVector(tuple.getBytes(ix)))

    implicit val ByteBufferTupleGetter: TupleGetter[ByteBuffer] = of[ByteBuffer](tuple => ix => tuple.getBytes(ix))

    implicit val ArrayByteTupleGetter: TupleGetter[Array[Byte]] = of[Array[Byte]](tuple => ix => ByteVector(tuple.getBytes(ix)).toArray)

    implicit val DateTupleGetter: TupleGetter[Date] = of[Date](tuple => ix => tuple.getDate(ix))

    implicit val InstantTupleGetter: TupleGetter[Instant] = of[Instant](tuple => ix => tuple.getDate(ix).toInstant)

    implicit val BigDecimalTupleGetter: TupleGetter[BigDecimal] = of[BigDecimal](tuple => ix => tuple.getDecimal(ix))

    implicit val JavaBigDecimalTupleGetter: TupleGetter[java.math.BigDecimal] = of[java.math.BigDecimal](tuple => ix => tuple.getDecimal(ix))

    implicit val IntTupleGetter: TupleGetter[Int] = of[Int](tuple => ix => tuple.getInt(ix))

    implicit val BoxedIntTupleGetter: TupleGetter[java.lang.Integer] = of[java.lang.Integer](tuple => ix => tuple.getInt(ix))

    implicit val LongTupleGetter: TupleGetter[Long] = of[Long](tuple => ix => tuple.getLong(ix))

    implicit val BoxedLongTupleGetter: TupleGetter[java.lang.Long] = of[java.lang.Long](tuple => ix => tuple.getLong(ix))

    implicit val FloatTupleGetter: TupleGetter[Float] = of[Float](tuple => ix => tuple.getFloat(ix))

    implicit val BoxedFloatTupleGetter: TupleGetter[java.lang.Float] = of[java.lang.Float](tuple => ix => tuple.getFloat(ix))

    implicit val DoubleTupleGetter: TupleGetter[Double] = of[Double](tuple => ix => tuple.getDouble(ix))

    implicit val BoxedDoubleTupleGetter: TupleGetter[java.lang.Double] = of[java.lang.Double](tuple => ix => tuple.getDouble(ix))

    implicit val InetTupleGetter: TupleGetter[InetAddress] = of[InetAddress](tuple => ix => tuple.getInet(ix))

    implicit val StringTupleGetter: TupleGetter[String] = of[String](tuple => ix => tuple.getString(ix))

    implicit val UUIDTupleGetter: TupleGetter[UUID] = of[UUID](tuple => ix => tuple.getUUID(ix))

    implicit val TupleValueTupleGetter: TupleGetter[TupleValue] = of[TupleValue](tuple => ix => tuple.getTupleValue(ix))

    implicit val UDTValueTupleGetter: TupleGetter[UDTValue] = of[UDTValue](tuple => ix => tuple.getUDTValue(ix))

    implicit def SeqTupleGetter[A]: TupleGetter[Seq[A]] = of[Seq[A]](tuple => ix => tuple.getList[A](ix, new TypeToken[A]() {}))

    implicit def JavaListTupleGetter[A]: TupleGetter[java.util.List[A]] = of[java.util.List[A]](tuple => ix => tuple.getList[A](ix, new TypeToken[A]() {}))

    implicit def SetTupleGetter[A]: TupleGetter[Set[A]] = of[Set[A]](tuple => ix => tuple.getSet[A](ix, new TypeToken[A]() {}).toSet)

    implicit def JavaSetTupleGetter[A]: TupleGetter[java.util.Set[A]] = of[java.util.Set[A]](tuple => ix => tuple.getSet[A](ix, new TypeToken[A]() {}))

    implicit def MapTupleGetter[K, V]: TupleGetter[Map[K, V]] = of[Map[K, V]](tuple => ix => tuple.getMap[K, V](ix, new TypeToken[K] {}, new TypeToken[V] {}).toMap)

    implicit def JavaMapTupleGetter[K, V]: TupleGetter[java.util.Map[K, V]] = of[java.util.Map[K, V]](tuple => ix => tuple.getMap[K, V](ix, new TypeToken[K] {}, new TypeToken[V] {}))

  }

}
