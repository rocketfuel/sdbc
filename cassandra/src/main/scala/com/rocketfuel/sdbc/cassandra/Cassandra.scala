package com.rocketfuel.sdbc.cassandra

import com.datastax.driver.core

private[sdbc] abstract class Cassandra
  extends ParameterValue
  with TupleParameterValues
  with TupleDataType
  with SessionMethods
  with StringContextMethods
  with Query
  with Queryable
  with RowConverter
  with CompositeGetter
  with TupleValue
  with TupleGetter
  with CompositeTupleGetter {

  type Session = core.Session

  type Cluster = core.Cluster

  type UDTValue = core.UDTValue

  type Token = core.Token

  type QueryOptions = com.rocketfuel.sdbc.cassandra.QueryOptions

  val QueryOptions = com.rocketfuel.sdbc.cassandra.QueryOptions

}
