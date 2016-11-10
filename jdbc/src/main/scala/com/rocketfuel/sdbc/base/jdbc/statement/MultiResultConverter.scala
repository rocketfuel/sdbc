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

  case class MultiResultConverter[A] private(
    impl: PreparedStatement => A,
    resultSetType: Option[Int] = None,
    resultSetConcurrency: Option[Int] = None,
    keepOpen: Boolean = false
  ) extends (PreparedStatement => A) {

    override def apply(v1: PreparedStatement): A =
      impl(v1)

    def getMoreResults(s: Statement): Unit =
      s.getMoreResults(
        if (keepOpen)
          Statement.KEEP_CURRENT_RESULT
        else Statement.CLOSE_CURRENT_RESULT
      )

  }

  object MultiResultConverter extends LowerPriorityMultiResultConverter {

    def apply[A](implicit statementConverter: MultiResultConverter[A]): MultiResultConverter[A] =
      statementConverter

    implicit lazy val results: MultiResultConverter[ResultSet] =
      MultiResultConverter(
        impl = StatementConverter.results
      )

    implicit lazy val immutableResults: MultiResultConverter[QueryResult.Iterator[ImmutableRow]] =
      MultiResultConverter(
        impl = v1 => QueryResult.Iterator(ImmutableRow.iterator(results(v1))),
        keepOpen = true
      )

    implicit lazy val connectedResults: MultiResultConverter[QueryResult.Iterator[ConnectedRow]] =
      MultiResultConverter(
        impl = v1 => QueryResult.Iterator(ConnectedRow.iterator(results(v1))),
        keepOpen = true
      )

    implicit lazy val updateableResults: MultiResultConverter[QueryResult.Iterator[UpdateableRow]] =
      MultiResultConverter(
        impl = v1 => QueryResult.Iterator(StatementConverter.updatableResults(v1)),
        resultSetType = Some(ResultSet.TYPE_SCROLL_SENSITIVE),
        resultSetConcurrency = Some(ResultSet.CONCUR_UPDATABLE),
        keepOpen = true
      )

    implicit lazy val unit: MultiResultConverter[QueryResult.Unit] =
      MultiResultConverter(
        impl = Function.const(QueryResult.Unit)
      )

    implicit lazy val update: MultiResultConverter[QueryResult.Update] =
      MultiResultConverter(
        impl = v1 => QueryResult.Update(StatementConverter.update(v1))
      )

    implicit def convertedRowIterator[A](implicit
      converter: RowConverter[A]
    ): MultiResultConverter[QueryResult.Iterator[A]] =
      MultiResultConverter(
        impl = v1 => QueryResult.Iterator(StatementConverter.convertedRowIterator(v1)),
        keepOpen = true
      )

    implicit def convertedRowVector[A](implicit
      converter: RowConverter[A]
    ): MultiResultConverter[QueryResult.Vector[A]] =
      MultiResultConverter(
        impl = v1 => QueryResult.Vector(StatementConverter.convertedRowVector(v1))
      )

    implicit def convertedRowOption[A](implicit
      converter: RowConverter[A]
    ): MultiResultConverter[QueryResult.Option[A]] =
      MultiResultConverter(
        impl = v1 => QueryResult.Option(StatementConverter.convertedRowOption(v1))
      )

    implicit def convertedRowSingleton[
      R <: Row,
      A
    ](implicit converter: RowConverter[A]
    ): MultiResultConverter[QueryResult.Singleton[A]] =
      MultiResultConverter(
        impl = v1 => QueryResult.Singleton(StatementConverter.convertedRowSingleton[A](v1))
      )

    implicit def recordComposite[
      H,
      T <: HList,
      K <: Symbol
    ](implicit
      H: MultiResultConverter[H],
      T: MultiResultConverter[T]
    ): MultiResultConverter[FieldType[K, H] :: T] =
      MultiResultConverter(
        impl = { v1 =>
          val head = H(v1)
          H.getMoreResults(v1)
          val tail = T(v1)
          field[K](head) :: tail
        },
        resultSetType = H.resultSetType.orElse(T.resultSetType),
        resultSetConcurrency = H.resultSetConcurrency.orElse(T.resultSetConcurrency)
      )

  }

  trait LowerPriorityMultiResultConverter {

    implicit lazy val emptyProduct: MultiResultConverter[HNil] =
      MultiResultConverter(
        impl = Function.const(HNil)
      )

    implicit def product[H, T <: HList](implicit
      H: MultiResultConverter[H],
      T: MultiResultConverter[T]
    ): MultiResultConverter[H :: T] =
      MultiResultConverter(
        impl = { v1 =>
          val h = H(v1)
          H.getMoreResults(v1)
          val t = T(v1)
          h :: t
        },
        resultSetType = H.resultSetType.orElse(T.resultSetType),
        resultSetConcurrency = H.resultSetConcurrency.orElse(T.resultSetConcurrency)
      )

    implicit def generic[F, G](implicit
      gen: Generic.Aux[F, G],
      G: MultiResultConverter[G]
    ): MultiResultConverter[F] =
      MultiResultConverter(
        impl = v1 => gen.from(G(v1))
      )

  }

}
