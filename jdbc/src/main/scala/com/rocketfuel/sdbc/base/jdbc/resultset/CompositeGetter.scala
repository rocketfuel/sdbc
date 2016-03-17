package com.rocketfuel.sdbc.base.jdbc.resultset

import com.rocketfuel.sdbc.base.jdbc.DBMS
import shapeless._
import shapeless.labelled._

trait CompositeGetter extends Getter {
  self: DBMS =>

  /**
    * Like doobie's Composite, but only the getter part.
    *
    * @tparam Row
    * @tparam A
    */
  trait CompositeGetter[-Row, A] extends ((Row, Index) => A) {

    val length: Int

  }

  /**
    * This is inspired from doobie, which supports using Shapeless to create getters, setters, and updaters.
    */
  object CompositeGetter extends LowerPriorityCompositeGetter {
    def apply[Row, A](implicit getter: CompositeGetter[Row, A]): CompositeGetter[Row, A] = getter

    implicit def optionFromGetter[Row, A](implicit g: Getter[Row, A]): CompositeGetter[Row, Option[A]] = {
      new CompositeGetter[Row, Option[A]] {
        override val length: Int = 1

        override def apply(v1: Row, v2: Index): Option[A] = {
          g(v1, v2)
        }
      }
    }

    implicit def fromGetter[Row, A](implicit g: Getter[Row, A]): CompositeGetter[Row, A] =
      new CompositeGetter[Row, A] {
        override def apply(v1: Row, v2: Index): A = {
          g(v1, v2).get
        }

        override val length: Int = 1
      }

    implicit def recordComposite[K <: Symbol, H, T <: HList, HTRowUpperBound, HRow, TRow, HH >: HRow <: HTRowUpperBound, TT >: TRow <: HTRowUpperBound](implicit
      H: CompositeGetter[HRow, H],
      T: CompositeGetter[TRow, T]
    ): CompositeGetter[HTRowUpperBound, FieldType[K, H] :: T] =
      new CompositeGetter[HTRowUpperBound, FieldType[K, H] :: T] {
        override def apply(row: HTRowUpperBound, ix: Index): FieldType[K, H] :: T = {
          val head = H(row.asInstanceOf[HRow], ix)
          val tail = T(row.asInstanceOf[TRow], ix + H.length)

          field[K](head) :: tail
        }

        override val length: Int = H.length + T.length
      }
  }

  trait LowerPriorityCompositeGetter {

    implicit def product[H, T <: HList, HTRowUpperBound, HRow, TRow, HH >: HRow <: HTRowUpperBound, TT >: TRow <: HTRowUpperBound](implicit
      H: CompositeGetter[HRow, H],
      T: CompositeGetter[TRow, T]
    ): CompositeGetter[HTRowUpperBound, H :: T] =
      new CompositeGetter[HTRowUpperBound, H :: T] {
        override def apply(row: HTRowUpperBound, ix: Index): H :: T = {
          val head = H(row.asInstanceOf[HRow], ix)
          val tail = T(row.asInstanceOf[TRow], ix + H.length)
          head :: tail
        }

        override val length: Int = H.length + T.length
      }

    implicit def emptyProduct[R]: CompositeGetter[Row, HNil] =
      new CompositeGetter[Row, HNil] {

        override def apply(v1: Row, v2: Index): HNil = {
          HNil
        }

        override val length: Int = 0
      }

    implicit def generic[Row, F, G](implicit
      gen: Generic.Aux[F, G],
      G: Lazy[CompositeGetter[Row, G]]
    ): CompositeGetter[Row, F] =
      new CompositeGetter[Row, F] {
        override def apply(row: Row, ix: Index): F = {
          gen.from(G.value(row, ix))
        }

        override val length: Int = G.value.length
      }

  }

}
