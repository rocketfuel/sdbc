package com.rocketfuel.sdbc.base.jdbc.statement

import com.rocketfuel.sdbc.base.jdbc.DBMS
import java.sql.{ResultSet, SQLFeatureNotSupportedException, Statement}
import shapeless.{::, HList, HNil}

trait MultiStatementConverter {
  self: DBMS =>

  trait MultiStatementConverter[A] extends (Statement => A)

  object MultiStatementConverter extends LowerPriorityMultiStatementConverter {

    def apply[A](implicit statementConverter: MultiStatementConverter[A]): MultiStatementConverter[A] =
      statementConverter

    implicit val unit: MultiStatementConverter[Unit] =
      new MultiStatementConverter[Unit] {
        override def apply(v1: Statement): Unit = {
          v1.getMoreResults()
          ()
        }
      }

    implicit val update: MultiStatementConverter[QueryResult.UpdateCount] = {
      (v1: Statement) =>
        val count = try {
          v1.getLargeUpdateCount
        } catch {
          case _: UnsupportedOperationException |
               _: SQLFeatureNotSupportedException =>
            v1.getUpdateCount.toLong
        }
        v1.getMoreResults()
        if (count == -1) None.get else QueryResult.UpdateCount(count)
    }

    implicit def convertedRowIterator[A](implicit
      converter: RowConverter[A],
      statementConverter: MultiStatementConverter[CloseableIterator[ConnectedRow]]
    ): MultiStatementConverter[Iterator[A]] = {
      (v1: Statement) =>
        statementConverter(v1).mapCloseable(converter)
    }

    implicit def convertedRowVector[A](implicit
      converter: RowConverter[A],
      statementConverter: MultiStatementConverter[CloseableIterator[ConnectedRow]]
    ): MultiStatementConverter[Vector[A]] = {
      (v1: Statement) =>
        val i = statementConverter(v1)
        try i.map(converter).toVector
        finally i.close()
    }

    implicit def convertedRowOption[A](implicit
      converter: RowConverter[A],
      statementConverter: MultiStatementConverter[CloseableIterator[ConnectedRow]]
    ): MultiStatementConverter[Option[A]] = {
      (v1: Statement) =>
        val i = statementConverter(v1)
        try i.map(converter).toStream.headOption
        finally i.close()
    }

    implicit def convertedRowSingleton[
      R <: Row,
      A
    ](implicit converter: RowConverter[A],
      statementConverter: MultiStatementConverter[CloseableIterator[ConnectedRow]]
    ): MultiStatementConverter[A] =  {
      (v1: Statement) =>
        val i = statementConverter(v1)
        try i.map(converter).toStream.head
        finally i.close()
    }

  }

  trait LowerPriorityMultiStatementConverter {
    implicit def ofFunction[A](f: Statement => A): MultiStatementConverter[A] =
      new MultiStatementConverter[A] {
        override def apply(v1: Statement): A = f(v1)
      }

    implicit val results: MultiStatementConverter[ResultSet] = {
      (v1: Statement) => {
        val results = Option(v1.getResultSet()).get
        v1.getMoreResults(Statement.KEEP_CURRENT_RESULT)
        results
      }
    }

    implicit val immutableResults: MultiStatementConverter[CloseableIterator[ImmutableRow]] = {
      (v1: Statement) => {
        ImmutableRow.iterator(results(v1))
      }
    }

    implicit val updatableResults: MultiStatementConverter[CloseableIterator[ConnectedRow]] = {
      (v1: Statement) => {
        ConnectedRow.iterator(results(v1))
      }
    }

    implicit val emptyProduct: MultiStatementConverter[HNil] =
      new MultiStatementConverter[HNil] {
        override def apply(v1: Statement): HNil = HNil
      }

    implicit def product[H, T <: HList](implicit
      H: MultiStatementConverter[H],
      T: MultiStatementConverter[T]
    ): MultiStatementConverter[H :: T] = {
      new MultiStatementConverter[H :: T] {
        override def apply(v1: Statement): H :: T = {
          H(v1) :: T(v1)
        }
      }
    }
  }

}
