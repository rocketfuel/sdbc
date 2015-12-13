package com.rocketfuel.sdbc.base.jdbc

import java.io.Closeable
import java.sql.ResultSet

trait ResultSetImplicits {
  self: MutableRow
    with UpdatableRow
    with Updater
    with ParameterValue
    with Row
    with Getter =>

  implicit class ResultSetToRowIterator(underlying: ResultSet) {

    /**
     * Get an iterator over the result set.
     * It closes itself when you reach the end, or when you call close().
     * Only one iterator is ever created for a result set.
     * If you want another iterator, execute the select statement again.
     * @return
     */
    def iterator(): Iterator[MutableRow] with Closeable = {

      new Iterator[MutableRow] with Closeable {

        val row = new MutableRow(underlying)

        override def close(): Unit = {
          underlying.close()
        }

        override def hasNext: Boolean = {
          val result = underlying.next()
          if (!result) {
            row.close()
          }
          result
        }

        override def next(): MutableRow = {
          row
        }

      }

    }

    /**
     * Get an iterator over the mutable result set.
     * It closes itself when you reach the end, or when you call close().
     * Only one iterator is ever created for a result set.
     * If you want another iterator, execute the select statement again.
     * @return
     */
    def updatableIterator(): Iterator[UpdatableRow] = {

      new Iterator[UpdatableRow] with Closeable {

        val row = new UpdatableRow(underlying)

        override def close(): Unit = {
          underlying.close()
        }

        override def hasNext: Boolean = {
          val result = underlying.next()
          if (!result) {
            row.close()
          }
          result
        }

        override def next(): UpdatableRow = {
          row
        }

      }

    }

  }

}
