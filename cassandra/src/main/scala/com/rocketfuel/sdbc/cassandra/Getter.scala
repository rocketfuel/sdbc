package com.rocketfuel.sdbc.cassandra

import com.datastax.driver.core.{LocalDate, UDTValue}
import com.google.common.reflect.TypeToken
import java.net.InetAddress
import java.util.{Date, UUID}
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scodec.bits.ByteVector

trait Getter extends com.rocketfuel.sdbc.base.Getter with com.rocketfuel.sdbc.base.RowConverter {

  override type Row = com.datastax.driver.core.Row

  def functionToGetter[T](getter: Row => Int => T): Getter[T] = {
    new Getter[T] {
      override def apply(row: Row, ix: Int): Option[T] = {
        if (row.isNull(ix))
          None
        else Some(getter(row)(ix))
      }
      override val length: Int = 1
    }
  }

  implicit val BooleanGetter: Getter[Boolean] = functionToGetter[Boolean](row => ix => row.getBool(ix))

  implicit val BoxedBooleanGetter: Getter[java.lang.Boolean] = functionToGetter[java.lang.Boolean](row => ix => row.getBool(ix))

  implicit val ByteVectorGetter: Getter[ByteVector] = functionToGetter[ByteVector](row => ix => ByteVector(row.getBytes(ix)))

  implicit val LocalDateGetter: Getter[LocalDate] = functionToGetter[LocalDate](row => ix => row.getDate(ix))

  implicit val DateGetter: Getter[Date] = functionToGetter[Date](row => ix => new Date(row.getDate(ix).getMillisSinceEpoch))

  implicit val BigDecimalGetter: Getter[BigDecimal] = functionToGetter[BigDecimal](row => ix => row.getDecimal(ix))

  implicit val JavaBigDecimalGetter: Getter[java.math.BigDecimal] = functionToGetter[java.math.BigDecimal](row => ix => row.getDecimal(ix))

  implicit val IntGetter: Getter[Int] = functionToGetter[Int](row => ix => row.getInt(ix))

  implicit val BoxedIntGetter: Getter[java.lang.Integer] = functionToGetter[java.lang.Integer](row => ix => row.getInt(ix))

  implicit val LongGetter: Getter[Long] = functionToGetter[Long](row => ix => row.getLong(ix))

  implicit val BoxedLongGetter: Getter[java.lang.Long] = functionToGetter[java.lang.Long](row => ix => row.getLong(ix))

  implicit val FloatGetter: Getter[Float] = functionToGetter[Float](row => ix => row.getFloat(ix))

  implicit val BoxedFloatGetter: Getter[java.lang.Float] = functionToGetter[java.lang.Float](row => ix => row.getFloat(ix))

  implicit val DoubleGetter: Getter[Double] = functionToGetter[Double](row => ix => row.getDouble(ix))

  implicit val BoxedDoubleGetter: Getter[java.lang.Double] = functionToGetter[java.lang.Double](row => ix => row.getDouble(ix))

  implicit val BigIntegerGetter: Getter[java.math.BigInteger] = functionToGetter[java.math.BigInteger](row => ix => row.getVarint(ix))

  implicit val InetGetter: Getter[InetAddress] = functionToGetter[InetAddress](row => ix => row.getInet(ix))

  implicit val StringGetter: Getter[String] = functionToGetter[String](row => ix => row.getString(ix))

  implicit val UUIDGetter: Getter[UUID] = functionToGetter[UUID](row => ix => row.getUUID(ix))

  implicit val TupleValueGetter: Getter[TupleValue] = functionToGetter[TupleValue](row => ix => row.getTupleValue(ix))

  implicit val UDTValueGetter: Getter[UDTValue] = functionToGetter[UDTValue](row => ix => row.getUDTValue(ix))

  implicit def seqGetter[A](implicit classTag: ClassTag[A]): Getter[collection.immutable.Seq[A]] =
    functionToGetter[collection.immutable.Seq[A]] { row => ix =>
        val token = TypeToken.of[A](classTag.runtimeClass.asInstanceOf[Class[A]])
        row.getList[A](ix, token).asScala.to[collection.immutable.Seq]
    }

  implicit def setGetter[A](implicit classTag: ClassTag[A]): Getter[Set[A]] =
    functionToGetter[Set[A]] { row => ix =>
      val token = TypeToken.of[A](classTag.runtimeClass.asInstanceOf[Class[A]])
      row.getSet[A](ix, token).asScala.toSet
    }

  implicit def mapGetter[K, V](implicit keyTag: ClassTag[K], valueTag: ClassTag[V]): Getter[Map[K, V]] =
    functionToGetter[Map[K, V]] { row => ix =>
      val keyToken = TypeToken.of[K](keyTag.runtimeClass.asInstanceOf[Class[K]])
      val valueToken = TypeToken.of[V](valueTag.runtimeClass.asInstanceOf[Class[V]])
      row.getMap[K, V](ix, keyToken, valueToken).asScala.toMap
    }

}
