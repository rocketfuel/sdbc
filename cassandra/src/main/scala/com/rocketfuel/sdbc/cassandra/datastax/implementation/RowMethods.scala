package com.rocketfuel.sdbc.cassandra.datastax.implementation

import com.datastax.driver.core.{Row => CRow}

private[sdbc] trait RowMethods {
  self: ParameterValues =>

  implicit class Row(underlying: CRow) {

    def get[T](ix: Index)(implicit getter: RowGetter[T]): Option[T] = {
      getter(underlying, ix)
    }

    def getParameters(implicit getter: RowGetter[ParameterValue]): IndexedSeq[Option[ParameterValue]] = {
      IndexedSeq.tabulate(underlying.getColumnDefinitions.size())(ix => get[ParameterValue](ix))
    }

    def getParametersByName(implicit getter: RowGetter[ParameterValue]): Map[String, Option[ParameterValue]] = {
      getParameters.zipWithIndex.foldLeft(Map.empty[String, Option[ParameterValue]]) {
        case (accum, (value, ix)) =>
          accum + (underlying.getColumnDefinitions.getName(ix) -> value)
      }
    }

  }

}
