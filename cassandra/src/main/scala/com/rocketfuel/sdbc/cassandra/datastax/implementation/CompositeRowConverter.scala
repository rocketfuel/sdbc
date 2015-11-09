package com.rocketfuel.sdbc.cassandra.datastax.implementation

import com.datastax.driver.core.Row
import shapeless._
import shapeless.labelled._

/**
  * Like doobie's Composite, but only the getter part.
  * @tparam A
  */
trait CompositeRowConverter[+A] extends Function2[Row, Index, A] {

  val length: Int

}

/**
  * This is inspired from doobie, which supports using Shapeless to create getters, setters, and updaters.
  */
object CompositeRowConverter extends LowerPriorityCompositeRowConverter {
  def apply[A](implicit converter: CompositeRowConverter[A]): CompositeRowConverter[A] = converter

  implicit def fromGetterOption[A](implicit g: RowGetter[A]): CompositeRowConverter[Option[A]] =
    new CompositeRowConverter[Option[A]] {

      override def apply(v1: Row, v2: Index): Option[A] = {
        g(v1, v2)
      }

      override val length: Int = 1
    }

  implicit def fromGetter[A](implicit g: RowGetter[A]): CompositeRowConverter[A] =
    new CompositeRowConverter[A] {
      override def apply(v1: Row, v2: Index): A = {
        g(v1, v2).get
      }

      override val length: Int = 1
    }

  implicit def recordComposite[K <: Symbol, H, T <: HList](implicit
    H: CompositeRowConverter[H],
    T: CompositeRowConverter[T]
  ): CompositeRowConverter[FieldType[K, H] :: T] =
    new CompositeRowConverter[FieldType[K, H]:: T] {
      override def apply(row: Row, ix: Index): FieldType[K, H]::T = {
        field[K](H(row, ix)) :: T(row, ix.asInstanceOf[IntIndex] + H.length)
      }

      override val length: Int = H.length + T.length
    }
}

trait LowerPriorityCompositeRowConverter {

  implicit def product[H, T <: HList](implicit
    H: CompositeRowConverter[H],
    T: CompositeRowConverter[T]
  ): CompositeRowConverter[H :: T] =
    new CompositeRowConverter[H :: T] {
      override def apply(row: Row, ix: Index): ::[H, T] = {
        H(row, ix) :: T(row, ix.asInstanceOf[IntIndex] + H.length)
      }

      override val length: Int = H.length + T.length
    }

  implicit def emptyProduct: CompositeRowConverter[HNil] =
    new CompositeRowConverter[HNil] {
      override def apply(row: Row, ix: Index): HNil = {
        HNil : HNil
      }

      override val length: Int = 0
    }

  implicit def generic[F, G](implicit
    gen: Generic.Aux[F, G],
    G: Lazy[CompositeRowConverter[G]]
  ): CompositeRowConverter[F] =
    new CompositeRowConverter[F] {
      override def apply(row: Row, ix: Index): F = {
        gen.from(G.value(row, ix))
      }

      override val length: Int = G.value.length
    }

}
