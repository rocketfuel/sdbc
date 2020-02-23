package com.rocketfuel.sdbc.base.jdbc.resultset

import com.rocketfuel.sdbc.base.CloseableIterator
import com.rocketfuel.sdbc.base.jdbc.DBMS
import java.sql.ResultSet
import scala.collection._

trait ResultSetImplicits {
  self: DBMS =>

  implicit def resultSetIterator(underlying: ResultSet): ResultSetIterator =
    new ResultSetIterator(underlying)

}

class ResultSetIterator(val underlying: ResultSet) extends AnyVal {

  /**
    * Get an iterator over the result set.
    * Iterators over the same result set will not
    * share elements.
    */
  def iterator(): CloseableIterator[ResultSet] = {
    /*
    `hasNext` is optional, but `next` is not. Make sure the ResultSet's `next` is
     only called once per iteration, regardless of whether `hasNext` was called
     or not.

     https://stackoverflow.com/questions/1870022/java-iterator-backed-by-a-resultset
     */
    val i = new Iterator[ResultSet] {
      private var calledNext = false
      private var _hasNext = false

      override def hasNext: Boolean = {
        if (!calledNext) {
          calledNext = true
          _hasNext = underlying.next()
        }
        _hasNext
      }

      override def next(): ResultSet = {
        if (!hasNext) {
          throw new NoSuchElementException("next on empty iterator")
        }
        // reset the state
        calledNext = false
        underlying
      }
    }

    new CloseableIterator[ResultSet](i, CloseableIterator.SingleCloseTracking(underlying)) {
      override def close(): Unit = {
        underlying.close()
      }
    }
  }

}
