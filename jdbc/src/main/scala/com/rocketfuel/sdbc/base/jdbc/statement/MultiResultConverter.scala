package com.rocketfuel.sdbc.base.jdbc.statement

import com.rocketfuel.sdbc.base.jdbc.{Connection, DBMS}
import java.sql.{ResultSet, Statement}
import shapeless._
import shapeless.labelled._
import shapeless.syntax.std.tuple._

trait MultiResultConverter {
  self: DBMS with Connection =>

  sealed trait QueryResult[A] {
    val get: A
  }

  object QueryResult {

    abstract class Unit private extends QueryResult[scala.Unit] {
      override val get: scala.Unit = ()
    }

    case object Unit extends Unit

    case class Update(
      override val get: Long
    ) extends QueryResult[Long]

    case class Iterator[A](
      get: CloseableIterator[A]
    ) extends QueryResult[CloseableIterator[A]]

    case class Vector[A](
      override val get: scala.Vector[A]
    ) extends QueryResult[scala.Vector[A]]

    case class Singleton[A](
      override val get: A
    ) extends QueryResult[A]

    case class Option[A](
      override val get: scala.Option[A]
    ) extends QueryResult[scala.Option[A]]

    /**
      * Allow a QueryResult to pose as its wrapped value.
      */
    implicit def toGet[A](result: QueryResult[A]): A =
      result.get

    object get extends Poly1 {
      implicit val caseUnit =
        at[Unit](_.get)

      implicit val caseUpdate =
        at[Update](_.get)

      implicit def caseIterator[A] =
        at[Iterator[A]](_.get)

      implicit def caseVector[A] =
        at[Vector[A]](_.get)

      implicit def caseSingleton[A] =
        at[Singleton[A]](_.get)

      implicit def caseOption[A] =
        at[Option[A]](_.get)
    }

  }

  sealed trait MultiResultConverter[A] extends (PreparedStatement => A) {
    val resultSetType: Option[Int] = None

    val resultSetConcurrency: Option[Int] = None

    val keepOpen: Boolean = false

    def moreArg =
      if (keepOpen)
        Statement.KEEP_CURRENT_RESULT
      else Statement.CLOSE_CURRENT_RESULT

    def getMoreResults(s: Statement): Unit =
      s.getMoreResults(moreArg)
  }

  object MultiResultConverter extends LowerPriorityMultiResultConverter {

    def apply[A](implicit statementConverter: MultiResultConverter[A]): MultiResultConverter[A] =
      statementConverter

    private implicit def ofFunction[A](f: PreparedStatement => A): MultiResultConverter[A] =
      new MultiResultConverter[A] {
        override def apply(v1: PreparedStatement): A = f(v1)
      }

    implicit val results: MultiResultConverter[ResultSet] =
      StatementConverter.results _

    implicit val immutableResults: MultiResultConverter[QueryResult.Iterator[ImmutableRow]] = {
      (v1: PreparedStatement) => {
        QueryResult.Iterator(ImmutableRow.iterator(results(v1)))
      }
    }

    implicit val connectedResults: MultiResultConverter[QueryResult.Iterator[ConnectedRow]] =
      new MultiResultConverter[QueryResult.Iterator[ConnectedRow]] {
        override def apply(v1: PreparedStatement): QueryResult.Iterator[ConnectedRow] =
          QueryResult.Iterator(ConnectedRow.iterator(results(v1)))

        override val keepOpen: Boolean = true
      }

    implicit val updateableResults: MultiResultConverter[QueryResult.Iterator[UpdateableRow]] =
      new MultiResultConverter[QueryResult.Iterator[UpdateableRow]] {
        override def apply(v1: PreparedStatement): QueryResult.Iterator[UpdateableRow] =
          QueryResult.Iterator(StatementConverter.updatableResults(v1))

        override val resultSetType: Option[Int] =
          Some(ResultSet.TYPE_SCROLL_SENSITIVE)

        override val resultSetConcurrency: Option[Int] =
          Some(ResultSet.CONCUR_UPDATABLE)

        override val keepOpen: Boolean = true
      }

    implicit val unit: MultiResultConverter[QueryResult.Unit] =
      new MultiResultConverter[QueryResult.Unit] {
        override def apply(v1: PreparedStatement): QueryResult.Unit =
          QueryResult.Unit
      }

    implicit val update: MultiResultConverter[QueryResult.Update] = {
      (v1: PreparedStatement) =>
        QueryResult.Update(StatementConverter.update(v1))
    }

    implicit def convertedRowIterator[A](implicit
      converter: RowConverter[A]
    ): MultiResultConverter[QueryResult.Iterator[A]] =
      new MultiResultConverter[QueryResult.Iterator[A]] {
        override def apply(v1: PreparedStatement): QueryResult.Iterator[A] =
          QueryResult.Iterator(StatementConverter.convertedRowIterator(v1))

        override val keepOpen: Boolean = true
      }

    implicit def convertedRowVector[A](implicit
      converter: RowConverter[A]
    ): MultiResultConverter[QueryResult.Vector[A]] = {
      (v1: PreparedStatement) =>
        QueryResult.Vector(StatementConverter.convertedRowVector(v1))
    }

    implicit def convertedRowOption[A](implicit
      converter: RowConverter[A]
    ): MultiResultConverter[QueryResult.Option[A]] = {
      (v1: PreparedStatement) =>
        QueryResult.Option(StatementConverter.convertedRowOption(v1))
    }

    implicit def convertedRowSingleton[
      R <: Row,
      A
    ](implicit converter: RowConverter[A]
    ): MultiResultConverter[QueryResult.Singleton[A]] =  {
      (v1: PreparedStatement) =>
        QueryResult.Singleton(StatementConverter.convertedRowSingleton[A](v1))
    }

    implicit def recordComposite[
      H,
      T <: HList,
      K <: Symbol
    ](implicit
      H: MultiResultConverter[H],
      T: MultiResultConverter[T]
    ): MultiResultConverter[FieldType[K, H] :: T] =
      new MultiResultConverter[FieldType[K, H] :: T] {
        override def apply(v1: PreparedStatement): ::[FieldType[K, H], T] = {
          val head = H(v1)
          H.getMoreResults(v1)
          val tail = T(v1)
          field[K](head) :: tail
        }

        override val resultSetType: Option[Int] =
          H.resultSetType.orElse(T.resultSetType)

        override val resultSetConcurrency: Option[Int] =
          H.resultSetConcurrency.orElse(T.resultSetConcurrency)
      }

  }

  trait LowerPriorityMultiResultConverter {

    implicit val emptyProduct: MultiResultConverter[HNil] =
      new MultiResultConverter[HNil] {
        override def apply(v1: PreparedStatement): HNil = HNil
      }

    implicit def product[H, T <: HList](implicit
      H: MultiResultConverter[H],
      T: MultiResultConverter[T]
    ): MultiResultConverter[H :: T] = {
      new MultiResultConverter[H :: T] {
        override def apply(v1: PreparedStatement): H :: T = {
          val h = H(v1)
          H.getMoreResults(v1)
          val t = T(v1)
          h :: t
        }

        override val resultSetType: Option[Int] =
          H.resultSetType.orElse(T.resultSetType)

        override val resultSetConcurrency: Option[Int] =
          H.resultSetConcurrency.orElse(T.resultSetConcurrency)
      }
    }

    implicit def generic[F, G](implicit
      gen: Generic.Aux[F, G],
      G: MultiResultConverter[G]
    ): MultiResultConverter[F] =
      new MultiResultConverter[F] {
        override def apply(v1: PreparedStatement): F = {
          gen.from(G(v1))
        }
      }

  }

}
