package com.rocketfuel.sdbc.base

import scala.annotation.implicitNotFound

trait RowConverter {
  self: Getter =>

  type Row

  //Override this with a trait containing any additional row converters.
  trait RowConverters

  /**
    * A row converter is a composite of getters.
    * @tparam A
    */
  @implicitNotFound("Define an implicit function from Row to A, or create the missing Getters for parts of your product or record.")
  trait RowConverter[A] extends (Row => A)

  object RowConverter extends RowConverters {

    def apply[A](implicit rowConverter: RowConverter[A]): RowConverter[A] = rowConverter

    implicit def fromFunction[A](implicit
      converter: Row => A
    ): RowConverter[A] =
      new RowConverter[A] {
        override def apply(row: Row): A = {
          converter(row)
        }
      }

  }

}
