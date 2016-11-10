package com.rocketfuel.sdbc.base.jdbc.statement

import com.rocketfuel.sdbc.base.CloseableIterator
import com.rocketfuel.sdbc.base.jdbc.DBMS
import java.sql.{ResultSet, SQLFeatureNotSupportedException}

trait StatementConverter {
  self: DBMS =>

  object StatementConverter {

    def update(v1: PreparedStatement): Long = {
      val count = try {
        v1.getLargeUpdateCount
      } catch {
        case _: UnsupportedOperationException |
             _: SQLFeatureNotSupportedException =>
          v1.getUpdateCount.toLong
      }
      if (count == -1L)
        throw new NoSuchElementException("query result is not an update count")
      else count
    }

    def convertedRowIterator[
      A
    ](v1: PreparedStatement
    )(implicit converter: RowConverter[A]
    ): CloseableIterator[A] = {
      connectedResults(v1).mapCloseable(converter)
    }

    def convertedRowVector[
      A
    ](v1: PreparedStatement
    )(implicit converter: RowConverter[A]
    ): Vector[A] = {
      val i = connectedResults(v1)
      try i.map(converter).toVector
      finally i.close()
    }

    def convertedRowOption[
      A
    ](v1: PreparedStatement
    )(implicit converter: RowConverter[A]
    ): Option[A] = {
      val i = connectedResults(v1)
      try {
        if (i.hasNext)
          Some(converter(i.next()))
        else None
      } finally i.close()
    }

    def convertedRowSingleton[
      A
    ](v1: PreparedStatement
    )(implicit converter: RowConverter[A]
    ): A =  {
      val i = connectedResults(v1)
      try {
        if (i.hasNext)
          converter(i.next())
        else throw new NoSuchElementException("empty ResultSet")
      } finally i.close()
    }

    def results(v1: PreparedStatement): ResultSet = {
      Option(v1.getResultSet()).getOrElse(
        throw new NoSuchElementException("query result is not a result set")
      )
    }

    def immutableResults(v1: PreparedStatement): Vector[ImmutableRow] = {
      ImmutableRow.iterator(results(v1)).toVector
    }

    def connectedResults(v1: PreparedStatement): CloseableIterator[ConnectedRow] = {
      val resultSet = results(v1)
      CloseableIterator(ConnectedRow.iterator(resultSet), resultSet.close)
    }

    def updatableResults(v1: PreparedStatement): CloseableIterator[UpdateableRow] = {
      val resultSet = results(v1)
      CloseableIterator(UpdateableRow.iterator(resultSet), resultSet.close)
    }

  }

}
