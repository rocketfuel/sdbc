package com.rocketfuel.sdbc.base.jdbc.resultset

import com.rocketfuel.sdbc.base.jdbc.DBMS
import java.io.Closeable
import java.sql.ResultSet

trait ResultSetImplicits {
  self: DBMS =>

  implicit class ResultSetIterator(underlying: ResultSet) {

    /**
     * Get an iterator over the mutable result set.
     * It closes itself when you reach the end, or when you call close().
     * Only one iterator is ever created for a result set.
     * If you want another iterator, execute the select statement again.
      *
      * @return
     */
    def iterator(): Iterator[ResultSet] with Closeable = {

      new Iterator[ResultSet] with Closeable {

        override def close(): Unit = {
          underlying.close()
        }

        override def hasNext: Boolean = {
          val result = underlying.next()
          if (!result) {
            underlying.close()
          }
          result
        }

        override def next(): ResultSet = {
          underlying
        }

      }

    }

  }

}
