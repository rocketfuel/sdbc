package com.rocketfuel.sdbc.base.jdbc.resultset

import com.rocketfuel.sdbc.base.jdbc.DBMS
import java.sql.ResultSet
import scala.annotation.implicitNotFound

trait RowConverter {
  self: DBMS =>

  @implicitNotFound("Import a DBMS or define a function from Row to A.")
  trait RowConverter[-R <: Row, A] extends (R => A)

  object RowConverter extends LowerPriorityRowConverterImplicits {

    def apply[R <: Row, A](implicit rowConverter: RowConverter[R, A]): RowConverter[R, A] = rowConverter

    implicit def fromFunction[R <: Row, A](implicit
      converter: R => A
    ): RowConverter[R, A] =
      new RowConverter[R, A] {
        override def apply(row: R): A = {
          converter(row)
        }
      }
  }

  implicit def fromResultSetConverter[A](implicit
    converter: ResultSet => A
  ): RowConverter[UpdatableRow, A] =
    new RowConverter[UpdatableRow, A] {
      override def apply(row: UpdatableRow): A = {
        converter(row)
      }
    }

  /**
    * Automatically generated row converters are to be used
    * only if there isn't an explicit row converter.
    */
  trait LowerPriorityRowConverterImplicits {
    implicit def fromComposite[R <: Row, A](implicit
      converter: CompositeGetter[R, A]
    ): RowConverter[R, A] =
      new RowConverter[R, A] {
        override def apply(row: R): A = {
          converter(row, 0)
        }
      }
  }

}
