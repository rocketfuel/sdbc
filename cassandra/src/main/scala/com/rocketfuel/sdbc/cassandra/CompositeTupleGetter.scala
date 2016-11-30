package com.rocketfuel.sdbc.cassandra

import shapeless._
import shapeless.labelled._

/**
  * Like doobie's Composite, but only the getter part.
  *
  * @tparam A
  */
trait CompositeTupleGetter[A] extends ((TupleValue, Int) => A) {

  val length: Int

}
/**
  * This is inspired from doobie, which supports using Shapeless to create getters, setters, and updaters.
  */
object CompositeTupleGetter extends LowerPriorityCompositeTupleGetter {
  def apply[A](implicit converter: CompositeTupleGetter[A]): CompositeTupleGetter[A] = converter

  implicit def fromGetterOption[A](implicit g: TupleGetter[A]): CompositeTupleGetter[Option[A]] =
    new CompositeTupleGetter[Option[A]] {

      override def apply(v1: TupleValue, v2: Int): Option[A] = {
        g(v1, v2)
      }

      override val length: Int = 1
    }

  implicit def fromGetter[A](implicit g: TupleGetter[A]): CompositeTupleGetter[A] =
    new CompositeTupleGetter[A] {
      override def apply(v1: TupleValue, v2: Int): A = {
        g(v1, v2).get
      }

      override val length: Int = 1
    }



  implicit def recordComposite[K <: Symbol, H, T <: HList](implicit
    H: CompositeTupleGetter[H],
    T: CompositeTupleGetter[T]
  ): CompositeTupleGetter[FieldType[K, H] :: T] =
    new CompositeTupleGetter[FieldType[K, H]:: T] {
      override def apply(row: TupleValue, ix: Int): FieldType[K, H]::T = {
        field[K](H(row, ix)) :: T(row, ix + H.length)
      }

      override val length: Int = H.length + T.length
    }
}

trait LowerPriorityCompositeTupleGetter {

  implicit def product[H, T <: HList](implicit
    H: CompositeTupleGetter[H],
    T: CompositeTupleGetter[T]
  ): CompositeTupleGetter[H :: T] =
    new CompositeTupleGetter[H :: T] {
      override def apply(row: TupleValue, ix: Int): ::[H, T] = {
        H(row, ix) :: T(row, ix + H.length)
      }

      override val length: Int = H.length + T.length
    }

  implicit def emptyProduct: CompositeTupleGetter[HNil] =
    new CompositeTupleGetter[HNil] {
      override def apply(row: TupleValue, ix: Int): HNil = {
        HNil : HNil
      }

      override val length: Int = 0
    }

  implicit def generic[F, G](implicit
    gen: Generic.Aux[F, G],
    G: Lazy[CompositeTupleGetter[G]]
  ): CompositeTupleGetter[F] =
    new CompositeTupleGetter[F] {
      override def apply(row: TupleValue, ix: Int): F = {
        gen.from(G.value(row, ix))
      }

      override val length: Int = G.value.length
    }

}
