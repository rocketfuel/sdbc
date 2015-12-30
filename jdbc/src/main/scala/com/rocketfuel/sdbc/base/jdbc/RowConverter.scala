package com.rocketfuel.sdbc.base.jdbc

import scala.annotation.implicitNotFound

trait RowConverter {
  self: DBMS =>

//  @implicitNotFound("Import a DBMS or define a function from Row to A.")
  trait RowConverter[A] extends (Row => A)

  object RowConverter extends LowerPriorityRowConverterImplicits {

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
    implicit def fromComposite[A](implicit
      converter: CompositeGetter[A]
    ): RowConverter[A] =
      new RowConverter[A] {
        override def apply(row: Row): A = {
          converter(row, 0)
        }
      }
  }

}
