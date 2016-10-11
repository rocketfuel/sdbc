package com.rocketfuel.sdbc.base.jdbc.resultset

import com.rocketfuel.sdbc.base.jdbc.DBMS
import scala.annotation.implicitNotFound

trait RowConverter {
  self: DBMS =>

  @implicitNotFound("Import a DBMS or define an implicit function from ConnectedRow to A.")
  trait RowConverter[A] extends (ConnectedRow => A)

  object RowConverter extends LowerPriorityRowConverterImplicits {

    def apply[A](implicit rowConverter: RowConverter[A]): RowConverter[A] = rowConverter

    implicit def fromFunction[A](implicit
      converter: ConnectedRow => A
    ): RowConverter[A] =
      new RowConverter[A] {
        override def apply(row: ConnectedRow): A = {
          converter(row)
        }
      }

  implicit val ImmutableRowConverter =
    new RowConverter[ImmutableRow] {
      override def apply(row: ConnectedRow): ImmutableRow =
        new ImmutableRow(
          columnNames = row.columnNames,
          columnIndexes = row.columnIndexes,
          getMetaData = row.getMetaData,
          getRow = row.getRow,
          toSeq = row.toSeq
        )
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
        override def apply(row: ConnectedRow): A = {
          converter(row, 0)
        }
      }
  }

}
