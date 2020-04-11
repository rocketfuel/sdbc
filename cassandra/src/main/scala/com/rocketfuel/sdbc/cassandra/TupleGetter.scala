package com.rocketfuel.sdbc.cassandra

import com.datastax.oss.driver.api.core.data.UdtValue
import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteBuffer
import java.time.{Instant, LocalDate, LocalTime}
import java.util.UUID
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scodec.bits.ByteVector

trait TupleGetter[A] extends ((TupleValue, Int) => Option[A])

object TupleGetter {
  implicit def of[A](getter: TupleValue => Int => A): TupleGetter[A] = {
    new TupleGetter[A] {
      override def apply(tuple: TupleValue, ix: Int): Option[A] = {
        if (tuple.isNull(ix))
          None
        else Some(getter(tuple)(ix))
      }
    }
  }

  implicit val BooleanTupleGetter: TupleGetter[Boolean] = of[Boolean](tuple => ix => tuple.getBoolean(ix))

  implicit val BoxedBooleanTupleGetter: TupleGetter[java.lang.Boolean] = of[java.lang.Boolean](tuple => ix => tuple.getBoolean(ix))

  implicit val ByteVectorTupleGetter: TupleGetter[ByteVector] = of[ByteVector](tuple => ix => ByteVector(tuple.getByteBuffer(ix)))

  implicit val ByteBufferTupleGetter: TupleGetter[ByteBuffer] = of[ByteBuffer](tuple => ix => tuple.getByteBuffer(ix))

  implicit val ArrayByteTupleGetter: TupleGetter[Array[Byte]] = of[Array[Byte]](tuple => ix => ByteVector(tuple.getByteBuffer(ix)).toArray)

  implicit val LocalDateTupleGetter: TupleGetter[LocalDate] = of[LocalDate](tuple => ix => tuple.getLocalDate(ix))

  implicit val LocalTimeTupleGetter: TupleGetter[LocalTime] = of[LocalTime](tuple => ix => tuple.getLocalTime(ix))

  implicit val InstantTupleGetter: TupleGetter[Instant] = of[Instant](tuple => ix => tuple.getInstant(ix))

  implicit val BigDecimalTupleGetter: TupleGetter[BigDecimal] = of[BigDecimal](tuple => ix => tuple.getBigDecimal(ix))

  implicit val JavaBigDecimalTupleGetter: TupleGetter[java.math.BigDecimal] = of[java.math.BigDecimal](tuple => ix => tuple.getBigDecimal(ix))

  implicit val BigIntegerTupleGetter: TupleGetter[BigInteger] = of[BigInteger](tuple => ix => tuple.getBigInteger(ix))

  implicit val IntTupleGetter: TupleGetter[Int] = of[Int](tuple => ix => tuple.getInt(ix))

  implicit val BoxedIntTupleGetter: TupleGetter[java.lang.Integer] = of[java.lang.Integer](tuple => ix => tuple.getInt(ix))

  implicit val LongTupleGetter: TupleGetter[Long] = of[Long](tuple => ix => tuple.getLong(ix))

  implicit val BoxedLongTupleGetter: TupleGetter[java.lang.Long] = of[java.lang.Long](tuple => ix => tuple.getLong(ix))

  implicit val FloatTupleGetter: TupleGetter[Float] = of[Float](tuple => ix => tuple.getFloat(ix))

  implicit val BoxedFloatTupleGetter: TupleGetter[java.lang.Float] = of[java.lang.Float](tuple => ix => tuple.getFloat(ix))

  implicit val DoubleTupleGetter: TupleGetter[Double] = of[Double](tuple => ix => tuple.getDouble(ix))

  implicit val BoxedDoubleTupleGetter: TupleGetter[java.lang.Double] = of[java.lang.Double](tuple => ix => tuple.getDouble(ix))

  implicit val InetTupleGetter: TupleGetter[InetAddress] = of[InetAddress](tuple => ix => tuple.getInetAddress(ix))

  implicit val StringTupleGetter: TupleGetter[String] = of[String](tuple => ix => tuple.getString(ix))

  implicit val UUIDTupleGetter: TupleGetter[UUID] = of[UUID](tuple => ix => tuple.getUuid(ix))

  implicit val TupleValueTupleGetter: TupleGetter[TupleValue] = of[TupleValue](tuple => ix => tuple.getTupleValue(ix))

  implicit val UDTValueTupleGetter: TupleGetter[UdtValue] = of[UdtValue](tuple => ix => tuple.getUdtValue(ix))

  implicit def SeqTupleGetter[A](implicit c: ClassTag[A]): TupleGetter[Seq[A]] = of[Seq[A]](tuple => ix => tuple.getList[A](ix, c.runtimeClass.asInstanceOf[Class[A]]).asScala.toSeq)

  implicit def JavaListTupleGetter[A](implicit c: ClassTag[A]): TupleGetter[java.util.List[A]] = of[java.util.List[A]](tuple => ix => tuple.getList[A](ix, c.runtimeClass.asInstanceOf[Class[A]]))

  implicit def SetTupleGetter[A](implicit c: ClassTag[A]): TupleGetter[Set[A]] = of[Set[A]](tuple => ix => tuple.getSet[A](ix, c.runtimeClass.asInstanceOf[Class[A]]).asScala.toSet)

  implicit def JavaSetTupleGetter[A](implicit c: ClassTag[A]): TupleGetter[java.util.Set[A]] = of[java.util.Set[A]](tuple => ix => tuple.getSet[A](ix, c.runtimeClass.asInstanceOf[Class[A]]))

  implicit def MapTupleGetter[K, V](implicit k: ClassTag[K], v: ClassTag[V]): TupleGetter[Map[K, V]] = of[Map[K, V]](tuple => ix => tuple.getMap[K, V](ix, k.runtimeClass.asInstanceOf[Class[K]], v.runtimeClass.asInstanceOf[Class[V]]).asScala.toMap)

  implicit def JavaMapTupleGetter[K, V](implicit k: ClassTag[K], v: ClassTag[V]): TupleGetter[java.util.Map[K, V]] = of[java.util.Map[K, V]](tuple => ix => tuple.getMap[K, V](ix, k.runtimeClass.asInstanceOf[Class[K]], v.runtimeClass.asInstanceOf[Class[V]]))

}
