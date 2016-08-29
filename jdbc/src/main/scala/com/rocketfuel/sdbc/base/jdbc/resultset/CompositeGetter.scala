package com.rocketfuel.sdbc.base.jdbc.resultset

import com.rocketfuel.sdbc.base.jdbc.DBMS
import shapeless._
import shapeless.labelled._

trait CompositeGetter extends Getter {
  self: DBMS =>

  /**
    * Like doobie's Composite, but only the getter part.
    *
    * @tparam A
    */
  trait CompositeGetter[A] extends ((Row, Index) => A) {

    val length: Int

  }

  /**
    * This is inspired from doobie, which supports using Shapeless to create getters, setters, and updaters.
    */
  object CompositeGetter extends LowerPriorityCompositeGetter {
    def apply[A](implicit getter: CompositeGetter[A]): CompositeGetter[A] = getter

    implicit def optionFromGetter[A](implicit g: Getter[A]): CompositeGetter[Option[A]] = {
      new CompositeGetter[Option[A]] {
        override val length: Int = 1

        override def apply(v1: Row, v2: Index): Option[A] = {
          g(v1, v2)
        }
      }
    }

    implicit def fromGetter[A](implicit g: Getter[A]): CompositeGetter[A] =
      new CompositeGetter[A] {
        override def apply(v1: Row, v2: Index): A = {
          g(v1, v2).get
        }

        override val length: Int = 1
      }

    implicit def recordComposite[K <: Symbol, H, T <: HList](implicit
      H: CompositeGetter[H],
      T: CompositeGetter[T]
    ): CompositeGetter[FieldType[K, H] :: T] =
      new CompositeGetter[FieldType[K, H] :: T] {
        override def apply(row: Row, ix: Index): FieldType[K, H] :: T = {
          val head = H(row, ix)
          val tail = T(row, ix + H.length)

          field[K](head) :: tail
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
        override def apply(row: Row, ix: Index): H :: T = {
          val head = H(row, ix)
          val tail = T(row, ix + H.length)
          head :: tail
        }

        override val length: Int = H.length + T.length
      }

    implicit val emptyProduct: CompositeGetter[HNil] =
      new CompositeGetter[HNil] {
        override def apply(v1: Row, v2: Index): HNil = {
          HNil
        }

        override val length: Int = 0
      }

    implicit def generic[F, G](implicit
      gen: Generic.Aux[F, G],
      G: Lazy[CompositeGetter[G]]
    ): CompositeGetter[F] =
      new CompositeGetter[F] {
        override def apply(row: Row, ix: Index): F = {
          gen.from(G.value(row, ix))
        }

        override val length: Int = G.value.length
      }

  }

}
