package com.rocketfuel.sdbc.cassandra

import com.datastax.driver.core.{LocalDate, Row, UDTValue}
import com.google.common.reflect.TypeToken
import java.net.InetAddress
import java.util.{Date, UUID}
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scodec.bits.ByteVector
import shapeless._
import shapeless.labelled._

/**
  * Like doobie's Composite, but only the getter part.
  * @tparam A
  */
trait CompositeGetter[A] extends ((Row, Int) => A) {

  val length: Int

}

/**
  * This is inspired from doobie, which supports using Shapeless to create getters, setters, and updaters.
  */
object CompositeGetter extends LowerPriorityCompositeGetter {
  def apply[A](implicit converter: CompositeGetter[A]): CompositeGetter[A] = converter

  implicit def of[T](getter: Row => Int => T): CompositeGetter[T] = {
    new CompositeGetter[T] {
      override def apply(row: Row, ix: Int): T = {
        getter(row)(ix)
      }
      override val length: Int = 1
    }
  }

  implicit val BooleanCompositeGetter: CompositeGetter[Boolean] = of[Boolean](row => ix => row.getBool(ix))

  implicit val BoxedBooleanCompositeGetter: CompositeGetter[java.lang.Boolean] = of[java.lang.Boolean](row => ix => row.getBool(ix))

  implicit val ByteVectorCompositeGetter: CompositeGetter[ByteVector] = of[ByteVector](row => ix => ByteVector(row.getBytes(ix)))

  implicit val LocalDateCompositeGetter: CompositeGetter[LocalDate] = of[LocalDate](row => ix => row.getDate(ix))

  implicit val DateCompositeGetter: CompositeGetter[Date] = of[Date](row => ix => new Date(row.getDate(ix).getMillisSinceEpoch))

  implicit val BigDecimalCompositeGetter: CompositeGetter[BigDecimal] = of[BigDecimal](row => ix => row.getDecimal(ix))

  implicit val JavaBigDecimalCompositeGetter: CompositeGetter[java.math.BigDecimal] = of[java.math.BigDecimal](row => ix => row.getDecimal(ix))

  implicit val IntCompositeGetter: CompositeGetter[Int] = of[Int](row => ix => row.getInt(ix))

  implicit val BoxedIntCompositeGetter: CompositeGetter[java.lang.Integer] = of[java.lang.Integer](row => ix => row.getInt(ix))

  implicit val LongCompositeGetter: CompositeGetter[Long] = of[Long](row => ix => row.getLong(ix))

  implicit val BoxedLongCompositeGetter: CompositeGetter[java.lang.Long] = of[java.lang.Long](row => ix => row.getLong(ix))

  implicit val FloatCompositeGetter: CompositeGetter[Float] = of[Float](row => ix => row.getFloat(ix))

  implicit val BoxedFloatCompositeGetter: CompositeGetter[java.lang.Float] = of[java.lang.Float](row => ix => row.getFloat(ix))

  implicit val DoubleCompositeGetter: CompositeGetter[Double] = of[Double](row => ix => row.getDouble(ix))

  implicit val BoxedDoubleCompositeGetter: CompositeGetter[java.lang.Double] = of[java.lang.Double](row => ix => row.getDouble(ix))

  implicit val BigIntegerCompositeGetter: CompositeGetter[java.math.BigInteger] = of[java.math.BigInteger](row => ix => row.getVarint(ix))

  implicit val InetCompositeGetter: CompositeGetter[InetAddress] = of[InetAddress](row => ix => row.getInet(ix))

  implicit val StringCompositeGetter: CompositeGetter[String] = of[String](row => ix => row.getString(ix))

  implicit val UUIDCompositeGetter: CompositeGetter[UUID] = of[UUID](row => ix => row.getUUID(ix))

  implicit val TupleValueCompositeGetter: CompositeGetter[TupleValue] = of[TupleValue](row => ix => row.getTupleValue(ix))

  implicit val UDTValueCompositeGetter: CompositeGetter[UDTValue] = of[UDTValue](row => ix => row.getUDTValue(ix))

  implicit def optional[A](implicit getter: CompositeGetter[A]): CompositeGetter[Option[A]] =
    new CompositeGetter[Option[A]] {
      override def apply(row: Row, ix: Int): Option[A] = {
        if (row.isNull(ix)) None
        else Some(getter(row, ix))
      }

      override val length: Int = 1
    }

  implicit def seq[A](implicit classTag: ClassTag[A]): CompositeGetter[collection.immutable.Seq[A]] = {
    new CompositeGetter[collection.immutable.Seq[A]] {
      override val length: Int = 1

      val token = TypeToken.of[A](classTag.runtimeClass.asInstanceOf[Class[A]])

      override def apply(row: Row, ix: Int): collection.immutable.Seq[A] = {
        row.getList[A](ix, token).asScala.to[collection.immutable.Seq]
      }
    }
  }

  implicit def set[A](implicit classTag: ClassTag[A]): CompositeGetter[Set[A]] = {
    new CompositeGetter[Set[A]] {
      override val length: Int = 1

      val token = TypeToken.of[A](classTag.runtimeClass.asInstanceOf[Class[A]])

      override def apply(row: Row, ix: Int): Set[A] = {
        row.getSet[A](ix, token).asScala.toSet
      }
    }
  }

  implicit def map[K, V](implicit keyTag: ClassTag[K], valueTag: ClassTag[V]): CompositeGetter[Map[K, V]] = {
    new CompositeGetter[Map[K, V]] {
      override val length: Int = 1

      val keyToken = TypeToken.of[K](keyTag.runtimeClass.asInstanceOf[Class[K]])

      val valueToken = TypeToken.of[V](valueTag.runtimeClass.asInstanceOf[Class[V]])

      override def apply(row: Row, ix: Int): Map[K, V] = {
        row.getMap[K, V](ix, keyToken, valueToken).asScala.toMap
      }
    }
  }

  implicit def recordComposite[K <: Symbol, H, T <: HList](implicit
    H: CompositeGetter[H],
    T: CompositeGetter[T]
  ): CompositeGetter[FieldType[K, H] :: T] =
    new CompositeGetter[FieldType[K, H]:: T] {
      override def apply(row: Row, ix: Int): FieldType[K, H]::T = {
        field[K](H(row, ix)) :: T(row, ix + H.length)
      }

      override val length: Int = H.length + T.length
    }
}

trait LowerPriorityCompositeGetter {

  implicit def product[H, T <: HList](implicit
    H: CompositeGetter[H],
    T: CompositeGetter[T]
  ): CompositeGetter[H :: T] =
    new CompositeGetter[H :: T] {
      override def apply(row: Row, ix: Int): ::[H, T] = {
        H(row, ix) :: T(row, ix + H.length)
      }

      override val length: Int = H.length + T.length
    }

  implicit def emptyProduct: CompositeGetter[HNil] =
    new CompositeGetter[HNil] {
      override def apply(row: Row, ix: Int): HNil = {
        HNil : HNil
      }

      override val length: Int = 0
    }

  implicit def generic[F, G](implicit
    gen: Generic.Aux[F, G],
    G: Lazy[CompositeGetter[G]]
  ): CompositeGetter[F] =
    new CompositeGetter[F] {
      override def apply(row: Row, ix: Int): F = {
        gen.from(G.value(row, ix))
      }

      override val length: Int = G.value.length
    }

}
