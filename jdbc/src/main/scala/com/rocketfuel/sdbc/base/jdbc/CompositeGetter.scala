package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base
import shapeless._
import shapeless.labelled._

trait CompositeGetter {
  self: Getter
    with Row
    with Index =>

  /**
    * Like doobie's Composite, but only the getter part.
    * @tparam A
    */
  trait CompositeGetter[A] extends base.Getter[Row, Index.Index, A] {

    def apply(row: Row, ix: Index.Index): A

    val length: Int

  }

  /**
    * This is inspired from doobie, which supports using Shapeless to create getters, setters, and updaters.
    */
  object CompositeGetter extends LowerPriorityCompositeGetter {
    def apply[A](implicit getter: CompositeGetter[A]): CompositeGetter[A] = getter

    implicit def fromGetter[A](implicit g: Getter[A]): CompositeGetter[A] =
      new CompositeGetter[A] {
        override def apply(v1: Row, v2: Index.Index): A = {
          g(v1, v2)
        }

        override val length: Int = 1
      }

    implicit def recordComposite[K <: Symbol, H, T <: HList](implicit
      H: CompositeGetter[H],
      T: CompositeGetter[T]
    ): CompositeGetter[FieldType[K, H] :: T] =
      new CompositeGetter[FieldType[K, H] :: T] {
        override def apply(row: Row, ix: Index.Index): FieldType[K, H] :: T = {
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
        override def apply(row: Row, ix: Index.Index): ::[H, T] = {
          H(row, ix) :: T(row, ix + H.length)
        }

        override val length: Int = H.length + T.length
      }

    implicit def emptyProduct: CompositeGetter[HNil] =
      new CompositeGetter[HNil] {
        override def apply(row: Row, ix: Index.Index): HNil = {
          HNil: HNil
        }

        override val length: Int = 0
      }

    implicit def generic[F, G](implicit
      gen: Generic.Aux[F, G],
      G: Lazy[CompositeGetter[G]]
    ): CompositeGetter[F] =
      new CompositeGetter[F] {
        override def apply(row: Row, ix: Index.Index): F = {
          gen.from(G.value(row, ix))
        }

        override val length: Int = G.value.length
      }

  }

}
