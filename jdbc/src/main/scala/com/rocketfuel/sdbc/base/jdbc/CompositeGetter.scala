package com.rocketfuel.sdbc.base.jdbc

import shapeless._
import shapeless.labelled.{ field, FieldType }

/**
 * Like doobie's Composite, but only the getter part.
 * @tparam A
 */
trait CompositeGetter[A] {
  self =>

  val getter: (Row, Int) => A

  val length: Int

}

/**
 * This is inspired from doobie, which supports using Shapeless to create getters, setters, and updaters.
 */
object CompositeGetter extends LowerPriorityCompositeGetterImplicits {

  def apply[A](implicit getter: CompositeGetter[A]): CompositeGetter[A] = getter

  implicit def fromGetterOption[A](implicit g: Getter[A]): CompositeGetter[Option[A]] =
    new CompositeGetter[Option[A]] {
      override val getter = (row: Row, ix: Int) => g(row, ix)
      override val length: Int = 1
    }

  implicit def fromGetter[A](implicit g: Getter[A]): CompositeGetter[A] =
    new CompositeGetter[A] {
      override val getter = (row: Row, ix: Int) => g(row, ix).get
      override val length: Int = 1
    }

  implicit def recordComposite[K <: Symbol, H, T <: HList](implicit
    H: CompositeGetter[H],
    T: CompositeGetter[T]
  ): CompositeGetter[FieldType[K, H] :: T] =
    new CompositeGetter[FieldType[K, H]:: T] {
      override val getter: (Row, Int) => FieldType[K, H]::T = {
        (row: Row, ix: Int) =>
          field[K](H.getter(row, ix)) :: T.getter(row, ix + H.length)
      }

      override val length: Int = H.length + T.length
    }

}

trait LowerPriorityCompositeGetterImplicits {

  implicit def product[H, T <: HList](implicit
    H: CompositeGetter[H],
    T: CompositeGetter[T]
  ): CompositeGetter[H :: T] =
    new CompositeGetter[H :: T] {
      override val getter: (Row, Int) => ::[H, T] = {
        (row: Row, ix: Int) =>
          H.getter(row, ix) :: T.getter(row, ix + H.length)
      }

      override val length: Int = H.length + T.length
    }

  implicit def emptyProduct: CompositeGetter[HNil] =
    new CompositeGetter[HNil] {
      override val getter: (Row, Int) => HNil = {
        (row: Row, ix: Int) =>
          HNil : HNil
      }

      override val length: Int = 0
    }

  implicit def generic[F, G](implicit
    gen: Generic.Aux[F, G],
    G: Lazy[CompositeGetter[G]]
  ): CompositeGetter[F] =
    new CompositeGetter[F] {
      override val getter: (Row, Int) => F = {
        (row: Row, ix: Int) =>
          gen.from(G.value.getter(row, ix))
      }

      override val length: Int = G.value.length
    }

}
