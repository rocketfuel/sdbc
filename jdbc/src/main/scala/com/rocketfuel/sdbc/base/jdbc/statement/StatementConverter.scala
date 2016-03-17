package com.rocketfuel.sdbc.base.jdbc.statement

import com.rocketfuel.sdbc.base.jdbc.DBMS
import java.sql.{SQLFeatureNotSupportedException, ResultSet, Statement}
import shapeless.{HNil, ::, HList}

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
               SQLFeatureNotSupportedException =>
            v1.getUpdateCount
        }
        v1.getMoreResults()
        if (count == -1) None.get else UpdateCount(count)
    }

    implicit def convertedRowIterator[
      R >: ImmutableRow,
      A
    ](implicit converter: RowConverter[R, A]
    ): StatementConverter[Iterator[A]] = {
      (v1: Statement) =>
        ImmutableRow.iterator(results(v1)).map(converter)
    }

    implicit def convertedRowVector[
      R >: ImmutableRow,
      A
    ](implicit converter: RowConverter[R, A]
    ): StatementConverter[Vector[A]] = {
      (v1: Statement) => {
        convertedRowIterator[R, A](converter)(v1).toVector
      }
    }

    implicit def convertedRowOption[
      R >: ImmutableRow,
      A
    ](implicit converter: RowConverter[R, A]
    ): StatementConverter[Option[A]] = {
      (v1: Statement) => {
        convertedRowIterator[R, A](converter)(v1).toStream.headOption
      }
    }

    implicit def convertedRowSingleton[
      R >: ImmutableRow,
      A
    ](implicit converter: RowConverter[R, A]
    ): StatementConverter[A] =  {
      (v1: Statement) => {
        convertedRowIterator[R, A](converter)(v1).toStream.head
      }
    }

    implicit def convertedUpdatableRowIterator[A](implicit
      converter: RowConverter[UpdatableRow, A]
    ): StatementConverter[Iterator[A]] = {
      (v1: Statement) => {
          UpdatableRow.iterator(results(v1)).map(converter)
        }
      }

    implicit def convertedUpdatableRowVector[A](implicit
      converter: RowConverter[UpdatableRow, A]
    ): StatementConverter[Vector[A]] = {
        (v1: Statement) => {
          convertedUpdatableRowIterator[A](converter)(v1).toVector
        }
      }

    implicit def convertedUpdatableRowOption[A](implicit
      converter: RowConverter[UpdatableRow, A]
    ): StatementConverter[Option[A]] = {
        (v1: Statement) => {
          convertedUpdatableRowIterator[A](converter)(v1).toStream.headOption
        }
      }

    implicit def convertedUpdatableRowSingleton[A](implicit
      converter: RowConverter[UpdatableRow, A]
    ): StatementConverter[A] = {
        (v1: Statement) => {
          convertedUpdatableRowIterator[A](converter)(v1).toStream.head
        }
      }

  }

  trait LowerPriorityStatementConverter {
    implicit def ofFunction[A](f: Statement => A): StatementConverter[A] =
      new StatementConverter[A] {
        override def apply(v1: Statement): A = f(v1)
      }

    implicit val results: StatementConverter[ResultSet] = {
        (v1: Statement) => {
          val results = Option(v1.getResultSet()).get
          v1.getMoreResults(Statement.KEEP_CURRENT_RESULT)
          results
        }
      }

    implicit val immutableResults: StatementConverter[ImmutableRow] = {
        (v1: Statement) => {
          ImmutableRow.iterator(results(v1))
        }
      }

    implicit val updatableResults: StatementConverter[UpdatableRow] = {
      (v1: Statement) => {
        UpdatableRow.iterator(results(v1))
      }
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
  }

}
