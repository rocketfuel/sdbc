package com.rocketfuel.sdbc.cassandra.implementation

import java.net.InetAddress
import java.util.{UUID, Date}
import com.rocketfuel.sdbc.base
import scodec.bits.ByteVector
import scala.collection.convert.wrapAsScala._
import com.google.common.reflect.TypeToken

import scala.reflect.ClassTag

trait RowGetter {
  self: Cassandra =>

  private[sdbc] trait RowGetter[+T] extends base.Getter[Row, Index, T]

  object RowGetter {

    implicit def of[T](getter: Row => Int => T): RowGetter[T] = {
      new RowGetter[T] {
        override def apply(row: Row, toIx: Index): Option[T] = {
          val ix = toIx(row)
          if (row.isNull(ix)) None
          else Some(getter(row)(ix))
        }
      }
    }

    def apply[T](implicit rowGetter: RowGetter[T]): RowGetter[T] = rowGetter

    implicit val BooleanRowGetter: RowGetter[Boolean] = of[Boolean](row => ix => row.getBool(ix))

    implicit val BoxedBooleanRowGetter: RowGetter[java.lang.Boolean] = of[java.lang.Boolean](row => ix => row.getBool(ix))

    implicit val ByteVectorRowGetter: RowGetter[ByteVector] = of[ByteVector](row => ix => ByteVector(row.getBytes(ix)))

    implicit val DateRowGetter: RowGetter[Date] = of[Date](row => ix => row.getDate(ix))

    implicit val BigDecimalRowGetter: RowGetter[BigDecimal] = of[BigDecimal](row => ix => row.getDecimal(ix))

    implicit val JavaBigDecimalRowGetter: RowGetter[java.math.BigDecimal] = of[java.math.BigDecimal](row => ix => row.getDecimal(ix))

    implicit val IntRowGetter: RowGetter[Int] = of[Int](row => ix => row.getInt(ix))

    implicit val BoxedIntRowGetter: RowGetter[java.lang.Integer] = of[java.lang.Integer](row => ix => row.getInt(ix))

    implicit val LongRowGetter: RowGetter[Long] = of[Long](row => ix => row.getLong(ix))

    implicit val BoxedLongRowGetter: RowGetter[java.lang.Long] = of[java.lang.Long](row => ix => row.getLong(ix))

    implicit val FloatRowGetter: RowGetter[Float] = of[Float](row => ix => row.getFloat(ix))

    implicit val BoxedFloatRowGetter: RowGetter[java.lang.Float] = of[java.lang.Float](row => ix => row.getFloat(ix))

    implicit val DoubleRowGetter: RowGetter[Double] = of[Double](row => ix => row.getDouble(ix))

    implicit val BoxedDoubleRowGetter: RowGetter[java.lang.Double] = of[java.lang.Double](row => ix => row.getDouble(ix))

    implicit val InetRowGetter: RowGetter[InetAddress] = of[InetAddress](row => ix => row.getInet(ix))

    implicit val StringRowGetter: RowGetter[String] = of[String](row => ix => row.getString(ix))

    implicit val UUIDRowGetter: RowGetter[UUID] = of[UUID](row => ix => row.getUUID(ix))

    implicit val TupleValueRowGetter: RowGetter[TupleValue] = of[TupleValue](row => ix => row.getTupleValue(ix))

    implicit val UDTValueRowGetter: RowGetter[UDTValue] = of[UDTValue](row => ix => row.getUDTValue(ix))

    implicit def SeqRowGetter[T](implicit tTag: ClassTag[T]): RowGetter[Seq[T]] =
      of[Seq[T]](row => ix => row.getList[T](ix, tTag.runtimeClass.asInstanceOf[Class[T]]))

    implicit def SetRowGetter[T](implicit tTag: ClassTag[T]): RowGetter[Set[T]] =
      of[Set[T]](row => ix => row.getSet[T](ix, TypeToken.of[T](tTag.runtimeClass.asInstanceOf[Class[T]])).toSet)

    /**
     * This function is broken when it's used implicitly, because valueTag always becomes ClassTag[Nothing], instead of ClassTag[V].
     * @param keyTag
     * @param valueTag
     * @tparam K
     * @tparam V
     * @return
     */
    def MapRowGetter[K, V](keyTag: ClassTag[K], valueTag: ClassTag[V]): RowGetter[Map[K, V]] =
      of[Map[K, V]](row => ix => row.getMap[K, V](ix, TypeToken.of[K](keyTag.runtimeClass.asInstanceOf[Class[K]]), TypeToken.of[V](valueTag.runtimeClass.asInstanceOf[Class[V]])).toMap)

  }

}
