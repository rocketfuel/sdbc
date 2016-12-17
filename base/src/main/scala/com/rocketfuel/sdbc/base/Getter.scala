package com.rocketfuel.sdbc.base

import shapeless.{::, Generic, HList, HNil, Lazy}
import shapeless.labelled.{FieldType, field}

/*
 * This is inspired from doobie, which supports using Shapeless to create getters, setters, and updaters.
 */
trait Getter {
  self: RowConverter =>

  trait RowConverters {
    /**
      * Automatically generated row converters are to be used
      * only if there isn't an explicit row converter.
      */
    implicit def fromCompositeGetter[A](implicit
      converter: CompositeGetter[A]
    ): RowConverter[A] =
      new RowConverter[A] {
        override def apply(row: Row): A = {
          converter(row, 0)
        }
      }
  }

  /**
    * Getters provide a uniform interface for any value that might be stored
    * in a row, when indexed by a String or Int.
    *
    * Often a row in a database doesn't correspond to exactly one primitive value.
    * Instead, the row decomposes into parts, which then compose into yet another
    * non-primitive value. Row => elements => case class or product
    */
  trait Getter[+A] extends ((Row, Int) => Option[A]) {

    val length: Int

  }

  object Getter {
    def apply[A](implicit getter: Getter[A]): Getter[A] = getter

    implicit def ofFunction[A](f: (Row, Int) => Option[A]): Getter[A] =
      new Getter[A] {
        override val length: Int = 1

        override def apply(v1: Row, v2: Int): Option[A] =
          f(v1, v2)
      }

    implicit def ofParser[A](parser: String => A)(implicit stringGetter: Getter[String]): Getter[A] = {
      (row: Row, index: Int) => stringGetter(row, index).map(parser)
    }
  }


  /**
    * Like doobie's Composite, but only the getter part.
    *
    * @tparam A
    */
  trait CompositeGetter[A] extends ((Row, Int) => A) {

    val length: Int

  }

  /**
    * Use any Getters in scope to create a Getter for multiple columns.
    */
  object CompositeGetter extends LowerPriorityCompositeGetter {
    def apply[A](implicit getter: CompositeGetter[A]): CompositeGetter[A] = getter

    implicit def optionFromGetter[A](implicit g: Getter[A]): CompositeGetter[Option[A]] = {
      new CompositeGetter[Option[A]] {
        override val length: Int = 1

        override def apply(v1: Row, v2: Int): Option[A] = {
          g(v1, v2)
        }
      }
    }

    implicit def fromGetter[A](implicit g: Getter[A]): CompositeGetter[A] =
      new CompositeGetter[A] {
        override def apply(v1: Row, v2: Int): A = {
          g(v1, v2).get
        }

        override val length: Int = 1
      }

    implicit def recordComposite[
      H,
      T <: HList,
      K <: Symbol
    ](implicit
      H: CompositeGetter[H],
      T: CompositeGetter[T]
    ): CompositeGetter[FieldType[K, H] :: T] =
      new CompositeGetter[FieldType[K, H] :: T] {
        override def apply(row: Row, ix: Int): FieldType[K, H] :: T = {
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
        override def apply(row: Row, ix: Int): H :: T = {
          val head = H(row, ix)
          val tail = T(row, ix + H.length)
          head :: tail
        }

        override val length: Int = H.length + T.length
      }

    implicit val emptyProduct: CompositeGetter[HNil] =
      new CompositeGetter[HNil] {
        override def apply(v1: Row, v2: Int): HNil = {
          HNil
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

}
