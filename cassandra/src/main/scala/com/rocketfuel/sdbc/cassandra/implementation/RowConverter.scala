package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core
import scala.annotation.implicitNotFound

trait RowConverter {
  self: Cassandra =>

  //@implicitNotFound("Define an implicit function from Row to A, or make A a Product (i.e., a tuple or case class).")
  trait RowConverter[+A] extends (core.Row => A)

  object RowConverter extends LowerPriorityRowConverterImplicits {
    def apply[A](implicit rowConverter: RowConverter[A]): RowConverter[A] = rowConverter

    implicit def fromFunction[A](implicit
      converter: core.Row => A
    ): RowConverter[A] =
      new RowConverter[A] {
        override def apply(row: core.Row): A = {
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
        override def apply(row: core.Row): A = {
          converter(row, 0)
        }
      }
  }

}
