package com.rocketfuel.sdbc.base.jdbc.statement

import com.rocketfuel.sdbc.base.jdbc.{Connection, DBMS}
import java.sql.{ResultSet, SQLFeatureNotSupportedException}

trait StatementConverter {
  self: DBMS with Connection =>

  sealed trait QueryResult[A] {
    val get: A
  }

  object QueryResult {
    case object Ignore extends QueryResult[Unit] {
      override val get: Unit = ()
    }

    case class UpdateCount(
      override val get: Long
    ) extends QueryResult[Long]

    case class Iterator[A](
      get: CloseableIterator[A],
      close: () => Unit
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

    implicit def toGet[A](result: QueryResult[A]): A =
      result.get
  }

  trait StatementConverter[A] extends (Statement => A) {
    def prepareStatement(statement: CompiledStatement)(implicit connection: Connection): Statement = {
      connection.prepareStatement(statement.queryText)
    }
  }

  object StatementConverter {

    def apply[A](implicit statementConverter: StatementConverter[A]): StatementConverter[A] =
      statementConverter

    implicit val unit: StatementConverter[Unit] =
      new StatementConverter[Unit] {
        override def apply(v1: Statement): Unit = {
          ()
        }
      }

    implicit val update: StatementConverter[QueryResult.UpdateCount] = {
      (v1: Statement) =>
        val count = try {
          v1.getLargeUpdateCount
        } catch {
          case _: UnsupportedOperationException |
               _: SQLFeatureNotSupportedException =>
            v1.getUpdateCount.toLong
        }
        if (count == -1L) throw new NoSuchElementException("query result is not an update count")
        else QueryResult.UpdateCount(count)
    }

    implicit def convertedRowIterator[
      A
    ](implicit converter: RowConverter[A]
    ): StatementConverter[CloseableIterator[A]] = {
      (v1: Statement) =>
        connectedResults(v1).mapCloseable(converter)
    }

    implicit def convertedRowVector[
      A
    ](implicit converter: RowConverter[A]
    ): StatementConverter[QueryResult.Vector[A]] = {
      (v1: Statement) =>
        val i = updatableResults(v1)
        try QueryResult.Vector(i.map(converter).toVector)
        finally i.close()
    }

    implicit def convertedRowOption[
      A
    ](implicit converter: RowConverter[A]
    ): StatementConverter[QueryResult.Option[A]] = {
      (v1: Statement) =>
        val i = updatableResults(v1)
        try {
          if (i.hasNext)
            QueryResult.Option(Some(converter(i.next())))
          else QueryResult.Option(None: Option[A])
        } finally i.close()
    }

    implicit def convertedRowSingleton[
      A
    ](implicit converter: RowConverter[A]
    ): StatementConverter[QueryResult.Singleton[A]] =  {
      (v1: Statement) =>
        val i = updatableResults(v1)
        try {
          if (i.hasNext) QueryResult.Singleton(converter(i.next()))
          else throw new NoSuchElementException("empty ResultSet")
        } finally i.close()
    }

    implicit def ofFunction[A](f: Statement => A): StatementConverter[A] =
      new StatementConverter[A] {
        override def apply(v1: Statement): A = f(v1)
      }

    implicit val results: StatementConverter[ResultSet] = {
      (v1: Statement) => {
        Option(v1.getResultSet()).get
      }
    }

    implicit val immutableResults: StatementConverter[QueryResult.Vector[ImmutableRow]] = {
      (v1: Statement) => {
        QueryResult.Vector(ImmutableRow.iterator(results(v1)).toVector)
      }
    }

    val connectedResults: StatementConverter[QueryResult.Iterator[ConnectedRow]] =
      new StatementConverter[QueryResult.Iterator[ConnectedRow]] {
        override def apply(v1: Statement): QueryResult.Iterator[ConnectedRow] = {
          val resultSet = results(v1)
          QueryResult.Iterator(ConnectedRow.iterator(resultSet), resultSet.close)
        }

        override def prepareStatement(statement: CompiledStatement)(implicit connection: Connection): Statement = {
          connection.prepareStatement(statement.queryText)
        }
      }

    implicit val updatableResults: StatementConverter[QueryResult.Iterator[UpdatableRow]] =
      new StatementConverter[QueryResult.Iterator[UpdatableRow]] {
        override def apply(v1: Statement): QueryResult.Iterator[UpdatableRow] = {
          val resultSet = results(v1)
          QueryResult.Iterator(UpdatableRow.iterator(resultSet), resultSet.close)
        }

        override def prepareStatement(statement: CompiledStatement)(implicit connection: Connection): Statement = {
          connection.prepareStatement(statement.queryText, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)
        }
      }
  }

}
