package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core.{ResultSet, Row}
import scala.annotation.implicitNotFound
import scala.collection.generic.CanBuildFrom

trait RowConverter {
  self: Cassandra =>

  @implicitNotFound("Define an implicit function from Row to A, or make A a Product (i.e., a tuple or case class).")
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

  trait ResultSetConverter[A] extends (ResultSet => A)

  object ResultSetConverter {

    def apply[A](implicit resultSetConverter: ResultSetConverter[A]): ResultSetConverter[A] = resultSetConverter

    implicit def fromFunction[A](implicit
      resultSetConverter: ResultSet => A
    ): ResultSetConverter[A] = {
      new ResultSetConverter[A] {
        override def apply(v1: ResultSet): A = {
          resultSetConverter(v1)
        }
      }
    }

    implicit val unit: ResultSetConverter[Unit] = {
      (resultSet: ResultSet) => ()
    }

    implicit val option: ResultSetConverter[Option[Row]] = {
      (rs: ResultSet) => Option(rs.one())
    }

    implicit val singleton: ResultSetConverter[Row] = {
      (rs: ResultSet) => option(rs).get
    }

    implicit val iterator: ResultSetConverter[Iterator[Row]] = {
      import collection.convert.decorateAsScala._
      (rs: ResultSet) => rs.iterator().asScala
    }

    implicit def buildFromIterator[F[_]](implicit
      cb: CanBuildFrom[Iterator[Row], Row, F[Row]]
    ): ResultSetConverter[F[Row]] = {
      def toF(rs: ResultSet): F[Row] = {
        val iterator = iterator(rs)
        val builder = cb(iterator)
        for (row <- iterator) builder += row
        builder.result()
      }

      fromFunction(toF)
    }

  }

}
