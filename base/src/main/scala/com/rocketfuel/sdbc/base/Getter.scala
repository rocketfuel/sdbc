package com.rocketfuel.sdbc.base

import shapeless._

/*
 * This is inspired from doobie, which supports using Shapeless to create getters, setters, and updaters.
 */
trait Getter {

  type Row

  /**
    * Getters provide a uniform interface for any value that might be stored
    * in a row, when indexed by a String or Int.
    *
    * Often a row in a database doesn't correspond to exactly one primitive value.
    * Instead, the row decomposes into parts, which then compose into yet another
    * non-primitive value. Row => elements => case class or product
    */
  trait Getter[A] extends ((Row, Index) => A) {

    val length: Int

  }

  //Override this with a trait having a collection of implicit Getters.
  type Getters

  object Getter extends Getters with LowerPriorityGetter {
    def apply[A](implicit getter: Getter[A]): Getter[A] = getter

    implicit def optionFromGetter[A](implicit g: Getter[A]): Getter[Option[A]] = {
      new Getter[Option[A]] {
        override val length: Int = 1

        override def apply(v1: Row, v2: Index): Option[A] = {
          g(v1, v2)
        }
      }
    }
  }

  trait LowerPriorityGetter {

    implicit def product[H, T <: HList](implicit
      H: Getter[H],
      T: Getter[T]
    ): Getter[H :: T] =
      new Getter[H :: T] {
        override def apply(row: Row, ix: Index): H :: T = {
          val head = H(row, ix)
          val tail = T(row, ix + H.length)
          head :: tail
        }

        override val length: Int = H.length + T.length
      }

    implicit val emptyProduct: Getter[HNil] =
      new Getter[HNil] {
        override def apply(v1: Row, v2: Index): HNil = {
          HNil
        }

        override val length: Int = 0
      }

    implicit def generic[F, G](implicit
      gen: Generic.Aux[F, G],
      G: Lazy[Getter[G]]
    ): Getter[F] =
      new Getter[F] {
        override def apply(row: Row, ix: Index): F = {
          gen.from(G.value(row, ix))
        }

        override val length: Int = G.value.length
      }

  }
  
}
