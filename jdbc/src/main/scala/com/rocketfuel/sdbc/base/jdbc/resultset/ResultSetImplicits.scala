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
    * Get an iterator over the mutable result set.
    * Iterators over the same result set will not
    * share elements.
    *
    * If you want two iterators for the same results,
    * execute the query and create a new iterator.
    */
  def iterator(): CloseableIterator[ResultSet] = {
    val i = new Iterator[ResultSet] {
      override def hasNext: Boolean =
        underlying.next()

      override def next(): ResultSet =
        underlying
    }

    new CloseableIterator[ResultSet](i) {
      override def close(): Unit = {
        underlying.close()
      }
    }
  }

}
