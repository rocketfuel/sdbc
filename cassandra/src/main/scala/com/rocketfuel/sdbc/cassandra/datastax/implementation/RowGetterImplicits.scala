package com.rocketfuel.sdbc.cassandra.datastax.implementation

import com.datastax.driver.core.{Row => CRow}

private[sdbc] trait RowGetterImplicits {
    self: RowMethods =>

  implicit def GetterToRowConverterOption[T](implicit getter: RowGetter[T]): CRow => Option[T] = { row =>
    getter(row, 0)
  }

  implicit def GetterToRowConverter[T](implicit getter: RowGetter[T]): CRow => T = { row =>
    getter(row, 0).get
  }

}
