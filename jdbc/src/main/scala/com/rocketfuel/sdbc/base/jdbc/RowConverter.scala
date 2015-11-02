package com.rocketfuel.sdbc.base.jdbc

import scala.annotation.implicitNotFound

@implicitNotFound("Import a DBMS or define a function from Row to A.")
trait RowConverter[A] extends Function[Row, A]

object RowConverter {
  implicit def fromComposite[A](implicit
    converter: CompositeGetter[A]
  ): RowConverter[A] = new RowConverter[A] {
    override def apply(row: Row): A = {
      converter.getter(row, 0)
    }
  }

  implicit def fromFunction[A](implicit
    converter: Row => A
  ): RowConverter[A] = new RowConverter[A] {
    override def apply(row: Row): A = {
      converter(row)
    }
  }
}
