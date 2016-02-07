package com.rocketfuel.sdbc.base.jdbc.resultset

import com.rocketfuel.sdbc.base.jdbc.DBMS
import java.io.Closeable
import java.sql.SQLFeatureNotSupportedException

trait StatementImplicits {
  self: DBMS =>

  sealed trait StatementResult

  object StatementResult {
    private[sdbc] def apply(statement: Statement, forUpdate: Boolean): Option[StatementResult] = {
        Option(statement.getResultSet) match {
        case Some(resultSet) =>
          Some {
            if (forUpdate) UpdatableResults(UpdatableRow.iterator(resultSet))
            else Results(ImmutableRow.iterator(resultSet).toVector)
          }
        case None =>
          val smallUpdateCount = statement.getUpdateCount
          if (smallUpdateCount >= 0) {
            val count =
              try {
                statement.getLargeUpdateCount
              } catch {
                case _: UnsupportedOperationException |
                     SQLFeatureNotSupportedException =>
                  smallUpdateCount.toLong
              }
            Some(UpdateCount(count))
          } else None
      }
    }
  }

  case class UpdateCount private (count: Long) extends StatementResult

  case class Results private (results: Vector[ImmutableRow]) extends StatementResult

  case class UpdatableResults private (results: Iterator[UpdatableRow]) extends StatementResult

  implicit class StatementIterator(underlying: Statement) {
    /**
      * Get an iterator over the Statement's results.
      * It closes itself when you reach the end, or when you call close().
      * More than one iterator may be created for a statement, as long as
      * close() is called first.
      *
      * @param forUpdate true if ResultSets should be treated as iterators
      *                  instead of immutable values. If true, then ResultSets
      *                  must be iterated through before going to the next value
      *                  in the statement's iterator. Otherwise, the ResultSet
      *                  will be closed when hasNext() is called.
      *
      * @return
      */
    def iterator(forUpdate: Boolean): Iterator[StatementResult] with Closeable = {

      underlying.execute()

      new Iterator[StatementResult] with Closeable {

        private var here: Option[StatementResult] = None

        def get: Option[StatementResult] = here

        override def close(): Unit = {
          underlying.close()
          here = None
        }

        override def hasNext: Boolean = {
          here = StatementResult(underlying, forUpdate)
          underlying.getMoreResults()

          if (here.isDefined) {
            true
          } else {
            underlying.close()
            false
          }
        }

        override def next(): StatementResult = {
          here.get
        }

      }

    }
  }

}
