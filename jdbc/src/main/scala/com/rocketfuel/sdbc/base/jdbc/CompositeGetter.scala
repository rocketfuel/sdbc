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

  /**
   * For ProductTypeClass[CompositeGetter]
   */
  def xmap[B](f: A => B, g: B => A): CompositeGetter[B] =
    new CompositeGetter[B] {
      override val getter: (Row, Int) => B = (row: Row, ix: Int) => f(self.getter(row, ix))
      override val length: Int = self.length
    }
}

/**
 * This is inspired from doobie, which supports using Shapeless to create getters, setters, and updaters.
 */
object CompositeGetter extends LowerPriorityCompositeGetter {

  def apply[A](implicit A: CompositeGetter[A]): CompositeGetter[A] = A

  def apply[A](implicit converter: Row => A): CompositeGetter[A]= {
    new CompositeGetter[A] {
      override val getter: (Row, Int) => A = (row: Row, _: Int) => converter(row)
      override val length: Int = 1
    }
  }

  implicit def optionFromGetter[A](implicit g: Getter[A]): CompositeGetter[Option[A]] =
    new CompositeGetter[Option[A]] {
      override val getter = (row: Row, ix: Int) => g(row, IntIndex(ix))
      override val length: Int = 1
    }

  implicit def fromGetter[A](implicit g: Getter[A]): CompositeGetter[A] =
    new CompositeGetter[A] {
      override val getter = (row: Row, ix: Int) => g(row, IntIndex(ix)).get
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

trait LowerPriorityCompositeGetter extends ProductTypeClassCompanion[CompositeGetter] {
  object typeClass extends ProductTypeClass[CompositeGetter] {
    override def product[H, T <: HList](ch: CompositeGetter[H], ct: CompositeGetter[T]): CompositeGetter[::[H, T]] =
      new CompositeGetter[H :: T] {
        override val getter: (Row, Int) => H :: T = {
          (row: Row, ix: Int) =>
            ch.getter(row, ix) :: ct.getter(row, ix + ch.length)
        }

        override val length: Int = ch.length + ct.length
      }

    override def emptyProduct: CompositeGetter[HNil] =
      new CompositeGetter[HNil] {
        override val getter: (Row, Int) => HNil = (_, _) => HNil
        override val length: Int = 1
      }

    override def project[F, G](instance: => CompositeGetter[G], to: (F) => G, from: (G) => F): CompositeGetter[F] = {
      instance.xmap(from, to)
    }
  }
}
