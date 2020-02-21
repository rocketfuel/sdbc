package com.rocketfuel.sdbc.cassandra

import com.datastax.oss.driver.api.core.data.UdtValue
import com.rocketfuel.sdbc.base
import java.net.InetAddress
import java.time.LocalDate
import java.util.UUID
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scodec.bits.ByteVector

trait Getter extends base.Getter with base.RowConverter {

  override type Row = com.datastax.oss.driver.api.core.cql.Row

  def functionToGetter[T](getter: Row => Int => T): Getter[T] = {
    (row: Row, ix: Int) =>
      if (row.isNull(ix))
        None
      else Some(getter(row)(ix))
  }

  implicit val BooleanGetter: Getter[Boolean] = functionToGetter[Boolean](row => ix => row.getBoolean(ix))

  implicit val BoxedBooleanGetter: Getter[java.lang.Boolean] = functionToGetter[java.lang.Boolean](row => ix => row.getBoolean(ix))

  implicit val ByteVectorGetter: Getter[ByteVector] = functionToGetter[ByteVector](row => ix => ByteVector(row.getByteBuffer(ix)))

  implicit val LocalDateGetter: Getter[LocalDate] = functionToGetter[LocalDate](row => ix => row.getLocalDate(ix))

  implicit val BigDecimalGetter: Getter[BigDecimal] = functionToGetter[BigDecimal](row => ix => row.getBigDecimal(ix))

  implicit val JavaBigDecimalGetter: Getter[java.math.BigDecimal] = functionToGetter[java.math.BigDecimal](row => ix => row.getBigDecimal(ix))

  implicit val IntGetter: Getter[Int] = functionToGetter[Int](row => ix => row.getInt(ix))

  implicit val BoxedIntGetter: Getter[java.lang.Integer] = functionToGetter[java.lang.Integer](row => ix => row.getInt(ix))

  implicit val LongGetter: Getter[Long] = functionToGetter[Long](row => ix => row.getLong(ix))

  implicit val BoxedLongGetter: Getter[java.lang.Long] = functionToGetter[java.lang.Long](row => ix => row.getLong(ix))

  implicit val FloatGetter: Getter[Float] = functionToGetter[Float](row => ix => row.getFloat(ix))

  implicit val BoxedFloatGetter: Getter[java.lang.Float] = functionToGetter[java.lang.Float](row => ix => row.getFloat(ix))

  implicit val DoubleGetter: Getter[Double] = functionToGetter[Double](row => ix => row.getDouble(ix))

  implicit val BoxedDoubleGetter: Getter[java.lang.Double] = functionToGetter[java.lang.Double](row => ix => row.getDouble(ix))

  implicit val BigIntegerGetter: Getter[java.math.BigInteger] = functionToGetter[java.math.BigInteger](row => ix => row.getBigInteger(ix))

  implicit val InetGetter: Getter[InetAddress] = functionToGetter[InetAddress](row => ix => row.getInetAddress(ix))

  implicit val StringGetter: Getter[String] = functionToGetter[String](row => ix => row.getString(ix))

  implicit val UUIDGetter: Getter[UUID] = functionToGetter[UUID](row => ix => row.getUuid(ix))

  implicit val TupleValueGetter: Getter[TupleValue] = functionToGetter[TupleValue](row => ix => row.getTupleValue(ix))

  implicit val UDTValueGetter: Getter[UdtValue] = functionToGetter[UdtValue](row => ix => row.getUdtValue(ix))

  implicit def seqGetter[A](implicit classTag: ClassTag[A]): Getter[collection.immutable.Seq[A]] =
    functionToGetter[collection.immutable.Seq[A]] { row => ix =>
        row.getList[A](ix, classTag.runtimeClass.asInstanceOf[Class[A]]).asScala.to(collection.immutable.Seq)
    }

  implicit def setGetter[A](implicit classTag: ClassTag[A]): Getter[Set[A]] =
    functionToGetter[Set[A]] { row => ix =>
      row.getSet[A](ix, classTag.runtimeClass.asInstanceOf[Class[A]]).asScala.toSet
    }

  implicit def mapGetter[K, V](implicit keyTag: ClassTag[K], valueTag: ClassTag[V]): Getter[Map[K, V]] = {
    functionToGetter[Map[K, V]] { row =>
      ix =>
        row.getMap[K, V](ix, keyTag.runtimeClass.asInstanceOf[Class[K]], valueTag.runtimeClass.asInstanceOf[Class[V]]).asScala.toMap
    }
  }

}
