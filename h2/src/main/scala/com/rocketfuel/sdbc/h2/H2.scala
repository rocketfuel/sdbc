package com.rocketfuel.sdbc.h2

import com.rocketfuel.sdbc.base.jdbc
import com.rocketfuel.sdbc.base.jdbc.resultset.{DefaultGetters, SeqGetter}
import com.rocketfuel.sdbc.base.jdbc.statement.{DefaultParameters, SeqParameter}
import org.h2.jdbc.JdbcSQLDataException

trait H2
  extends jdbc.DBMS
  with DefaultGetters
  with DefaultParameters
  with jdbc.DefaultUpdaters
  with SeqParameter
  with SeqGetter
  with ArrayTypes
  with SerializedParameter
  with jdbc.JdbcConnection {

  type Serialized = com.rocketfuel.sdbc.h2.Serialized
  val Serialized = com.rocketfuel.sdbc.h2.Serialized

  /*
  // The H2 client has a bug when the array is empty.
  https://github.com/h2database/h2database/issues/2460
   */
  override implicit def toSeqGetter[T](implicit getter: Getter[T]): Getter[Seq[T]] = {
    (row: ConnectedRow, ix: Int) =>
      for {
        a <- Option(row.getArray(ix))
      } yield {
        try {
          val arrayIterator = ConnectedRow.iterator(a.getResultSet())
          val arrayValues = for {
            arrayRow <- arrayIterator
          } yield {
            arrayRow[T](1)
          }
          arrayValues.toVector
        } catch {
          case e: JdbcSQLDataException if e.getMessage == "Invalid value \"1\" for parameter \"index (1..0)\" [90008-200]" =>
            Vector.empty
        }
      }
  }

}
