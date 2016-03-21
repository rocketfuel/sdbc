package com.rocketfuel.sdbc.base.jdbc.resultset

import com.rocketfuel.sdbc.base.jdbc.DBMS

import scala.annotation.implicitNotFound

trait RowConverter {
  self: DBMS =>

  @implicitNotFound("Import a DBMS or define an implicit function from a row type to A. Examples of row types are ResultSet, ImmutableRow, or UpdatableRow.")
  trait RowConverter[-R, A] extends (R => A)

  object RowConverter extends LowerPriorityRowConverterImplicits {

    def apply[R, A](implicit rowConverter: RowConverter[R, A]): RowConverter[R, A] = rowConverter

    implicit def fromFunction[R, A](implicit
      converter: R => A
    ): RowConverter[R, A] =
      new RowConverter[R, A] {
        override def apply(row: R): A = {
          converter(row)
        }
      }
  }

  /**
    * Automatically generated row converters are to be used
    * only if there isn't an explicit row converter.
    */
  trait LowerPriorityRowConverterImplicits {
    implicit def fromComposite[R, A](implicit
      converter: CompositeGetter[R, A]
    ): RowConverter[R, A] =
      new RowConverter[R, A] {
        override def apply(row: R): A = {
          converter(row, 0)
        }
      }
  }

}
