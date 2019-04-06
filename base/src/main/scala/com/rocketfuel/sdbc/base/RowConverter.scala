package com.rocketfuel.sdbc.base

import scala.annotation.implicitNotFound

trait RowConverter {
  self: Getter =>

  type Row

  /**
    * A row converter is a composite of getters.
    * @tparam A
    */
  @implicitNotFound("Define an implicit function from Row to A, or create the missing Getters for parts of your product or record.")
  trait RowConverter[A] extends (Row => A)

  object RowConverter extends LowPriorityRowConverters {

    def apply[A](implicit rowConverter: RowConverter[A]): RowConverter[A] = rowConverter

    implicit def fromFunction[A](implicit
      converter: Row => A
    ): RowConverter[A] =
      new RowConverter[A] {
        override def apply(row: Row): A = {
          converter(row)
        }
      }

    implicit def fromExplicitFunction[A](
      converter: Row => A
    ): RowConverter[A] =
      new RowConverter[A] {
        override def apply(row: Row): A = {
          converter(row)
        }
      }

  }

  trait LowPriorityRowConverters {

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

}
