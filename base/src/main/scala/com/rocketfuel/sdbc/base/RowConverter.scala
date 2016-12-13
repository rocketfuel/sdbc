package com.rocketfuel.sdbc.base

import scala.annotation.implicitNotFound
import shapeless.{::, Generic, HList, HNil, Lazy}
import shapeless.labelled.{FieldType, field}

class RowConverter {
  self: Getter =>

  //Override this with a trait containing any additional row converters.
  trait RowConverters

  /**
    * A row converter is a composite of getters.
    * @tparam A
    */
  @implicitNotFound("Define an implicit function from ConnectedRow to A, or create the missing Getters for parts of your product or record.")
  trait RowConverter[A] extends (Row => A)
  
  object RowConverter extends RowConverters with LowerPriorityRowConverterImplicits {

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

  /**
    * Automatically generated row converters are to be used
    * only if there isn't an explicit row converter.
    */
  trait LowerPriorityRowConverterImplicits {
    implicit def fromGetter[A](implicit
      converter: Getter[A]
    ): RowConverter[A] =
      new RowConverter[A] {
        override def apply(row: Row): A = {
          converter(row, 0)
        }
      }
  }

}
