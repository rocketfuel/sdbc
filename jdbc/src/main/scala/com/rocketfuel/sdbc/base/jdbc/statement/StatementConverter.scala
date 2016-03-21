package com.rocketfuel.sdbc.base.jdbc.statement

import com.rocketfuel.sdbc.base.jdbc.DBMS
import java.sql._
import scala.collection.generic.CanBuildFrom
import shapeless.{::, Generic, HList, HNil, Lazy}

trait StatementConverter {
  self: DBMS =>

  trait StatementConverter[A] extends (Statement => A)

  case class UpdateCount(count: Long)

  object StatementConverter extends LowerPriorityStatementConverter {

    def apply[A](implicit statementConverter: StatementConverter[A]): StatementConverter[A] = statementConverter

    implicit val unit: StatementConverter[Unit] =
      new StatementConverter[Unit] {
        override def apply(v1: Statement): Unit = {
          v1.getMoreResults()
          ()
        }
      }

    implicit val update: StatementConverter[UpdateCount] = {
      (v1: Statement) =>
        val count = try {
          v1.getLargeUpdateCount
        } catch {
          case _: UnsupportedOperationException |
               _: SQLFeatureNotSupportedException =>
            v1.getUpdateCount.toLong
        }
        v1.getMoreResults()
        if (count == -1) None.get else UpdateCount(count)
    }

    implicit def convertedRowIterator[
      R >: ImmutableRow <: Row,
      A
    ](implicit converter: RowConverter[R, A]
    ): StatementConverter[Iterator[A]] = {
      (v1: Statement) =>
        ImmutableRow.iterator(results(v1)).map(converter)
    }

    implicit def convertedRowCanBuildFrom[
      R >: ImmutableRow <: Row,
      A,
      F[_]
    ](implicit converter: RowConverter[R, A],
      canBuildFrom: CanBuildFrom[Nothing, A, F[A]]
    ): StatementConverter[F[A]] = {
      (v1: Statement) =>
        val iterator = convertedRowIterator[R, A](converter)(v1)
        val builder = canBuildFrom()

        builder ++= iterator

        builder.result()
    }

    implicit def convertedRowOption[
      R >: ImmutableRow,
      A
    ](implicit converter: RowConverter[R, A]
    ): StatementConverter[Option[A]] = {
      (v1: Statement) =>
        resultSetOption(v1).map(ImmutableRow.apply _ andThen converter)
    }

    implicit def convertedRowSingleton[
      R >: ImmutableRow,
      A
    ](implicit converter: RowConverter[R, A]
    ): StatementConverter[A] =  {
      (v1: Statement) =>
        converter(ImmutableRow(resultSetSingleton(v1)))
    }

    implicit val updatableRowIterator: StatementConverter[Iterator[UpdatableRow]] = {
      (v1: Statement) =>
        UpdatableRow.iterator(results(v1))
    }

    implicit val updatableRowOption: StatementConverter[Option[UpdatableRow]] = {
      (v1: Statement) =>
        resultSetOption(v1).map(UpdatableRow.apply)
    }

    implicit val updatableRowSingleton: StatementConverter[UpdatableRow] = {
      (v1: Statement) =>
        UpdatableRow(resultSetSingleton(v1))
    }

  }

  trait LowerPriorityStatementConverter {
    implicit def ofFunction[A](f: Statement => A): StatementConverter[A] =
      new StatementConverter[A] {
        override def apply(v1: Statement): A = f(v1)
      }

    implicit val results: StatementConverter[ResultSet] = {
        (v1: Statement) =>
          val results = Option(v1.getResultSet()).get
          v1.getMoreResults(Statement.KEEP_CURRENT_RESULT)
          results
      }

    private[jdbc] val resultSetOption: StatementConverter[Option[ResultSet]] = {
      (v1: Statement) =>
        val results = Option(v1.getResultSet()).get
        try {
          if (results.next()) {
            Some(results)
          } else {
            None
          }
        } finally v1.getMoreResults()
    }

    private[jdbc] val resultSetSingleton: StatementConverter[ResultSet] =  {
      (v1: Statement) =>
        resultSetOption(v1).get
    }

    implicit val emptyProduct: StatementConverter[HNil] =
      new StatementConverter[HNil] {
        override def apply(v1: Statement): HNil = HNil
      }

    implicit def product[H, T <: HList](implicit
      H: StatementConverter[H],
      T: StatementConverter[T]
    ): StatementConverter[H :: T] = {
      new StatementConverter[H :: T] {
        override def apply(v1: Statement): H :: T = {
          H(v1) :: T(v1)
        }
      }
    }

    implicit def generic[F, G](implicit
      gen: Generic.Aux[F, G],
      G: Lazy[StatementConverter[G]]
    ): StatementConverter[F] =
      new StatementConverter[F] {
        override def apply(v1: Statement): F = {
          gen.from(G.value(v1))
        }
      }
  }

}
