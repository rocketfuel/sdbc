package com.rocketfuel.sdbc.cassandra

import com.datastax.driver.core.{LocalDate, UDTValue}
import com.google.common.reflect.TypeToken
import com.rocketfuel.sdbc.base.Index
import java.net.InetAddress
import java.util.{Date, UUID}
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scodec.bits.ByteVector

trait Getter extends com.rocketfuel.sdbc.base.Getter {

  override type Row = com.datastax.driver.core.Row

  trait Getters {
    implicit def of[T](getter: Row => Int => T): Getter[T] = {
      new Getter[T] {
        override def apply(row: Row, ix: Index): Option[T] = {
          val intIx = ix(row)
          if (row.isNull(intIx))
            None
          else Some(getter(row)(intIx))
        }
        override val length: Int = 1
      }
    }

    implicit val BooleanGetter: Getter[Boolean] = of[Boolean](row => ix => row.getBool(ix))

    implicit val BoxedBooleanGetter: Getter[java.lang.Boolean] = of[java.lang.Boolean](row => ix => row.getBool(ix))

    implicit val ByteVectorGetter: Getter[ByteVector] = of[ByteVector](row => ix => ByteVector(row.getBytes(ix)))

    implicit val LocalDateGetter: Getter[LocalDate] = of[LocalDate](row => ix => row.getDate(ix))

    implicit val DateGetter: Getter[Date] = of[Date](row => ix => new Date(row.getDate(ix).getMillisSinceEpoch))

    implicit val BigDecimalGetter: Getter[BigDecimal] = of[BigDecimal](row => ix => row.getDecimal(ix))

    implicit val JavaBigDecimalGetter: Getter[java.math.BigDecimal] = of[java.math.BigDecimal](row => ix => row.getDecimal(ix))

    implicit val IntGetter: Getter[Int] = of[Int](row => ix => row.getInt(ix))

    implicit val BoxedIntGetter: Getter[java.lang.Integer] = of[java.lang.Integer](row => ix => row.getInt(ix))

    implicit val LongGetter: Getter[Long] = of[Long](row => ix => row.getLong(ix))

    implicit val BoxedLongGetter: Getter[java.lang.Long] = of[java.lang.Long](row => ix => row.getLong(ix))

    implicit val FloatGetter: Getter[Float] = of[Float](row => ix => row.getFloat(ix))

    implicit val BoxedFloatGetter: Getter[java.lang.Float] = of[java.lang.Float](row => ix => row.getFloat(ix))

    implicit val DoubleGetter: Getter[Double] = of[Double](row => ix => row.getDouble(ix))

    implicit val BoxedDoubleGetter: Getter[java.lang.Double] = of[java.lang.Double](row => ix => row.getDouble(ix))

    implicit val BigIntegerGetter: Getter[java.math.BigInteger] = of[java.math.BigInteger](row => ix => row.getVarint(ix))

    implicit val InetGetter: Getter[InetAddress] = of[InetAddress](row => ix => row.getInet(ix))

    implicit val StringGetter: Getter[String] = of[String](row => ix => row.getString(ix))

    implicit val UUIDGetter: Getter[UUID] = of[UUID](row => ix => row.getUUID(ix))

    implicit val TupleValueGetter: Getter[TupleValue] = of[TupleValue](row => ix => row.getTupleValue(ix))

    implicit val UDTValueGetter: Getter[UDTValue] = of[UDTValue](row => ix => row.getUDTValue(ix))

    implicit def seq[A](implicit classTag: ClassTag[A]): Getter[collection.immutable.Seq[A]] = {
      new Getter[collection.immutable.Seq[A]] {
        override val length: Int = 1

        val token = TypeToken.of[A](classTag.runtimeClass.asInstanceOf[Class[A]])

        override def apply(row: Row, ix: Index): collection.immutable.Seq[A] = {
          row.getList[A](ix(row), token).asScala.to[collection.immutable.Seq]
        }
      }
    }

    implicit def set[A](implicit classTag: ClassTag[A]): Getter[Set[A]] = {
      new Getter[Set[A]] {
        override val length: Int = 1

        val token = TypeToken.of[A](classTag.runtimeClass.asInstanceOf[Class[A]])

        override def apply(row: Row, ix: Index): Set[A] = {
          row.getSet[A](ix(row), token).asScala.toSet
        }
      }
    }

    implicit def map[K, V](implicit keyTag: ClassTag[K], valueTag: ClassTag[V]): Getter[Map[K, V]] = {
      new Getter[Map[K, V]] {
        override val length: Int = 1

        val keyToken = TypeToken.of[K](keyTag.runtimeClass.asInstanceOf[Class[K]])

        val valueToken = TypeToken.of[V](valueTag.runtimeClass.asInstanceOf[Class[V]])

        override def apply(row: Row, ix: Index): Map[K, V] = {
          row.getMap[K, V](ix(row), keyToken, valueToken).asScala.toMap
        }
      }
    }
  }
}
