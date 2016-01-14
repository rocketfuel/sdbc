package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core

private[sdbc] abstract class Cassandra
  extends ParameterValue
  with TupleParameterValues
  with TupleDataType
  with SessionMethods
  with Executable
  with Selectable
  with StringContextMethods
  with Select
  with Execute
  with CassandraProcess
  with Row
  with RowConverter
  with CompositeGetter
  with TupleValue
  with TupleGetter
  with CompositeTupleGetter {

  type Session = core.Session

  type Cluster = core.Cluster

  type UDTValue = core.UDTValue

  type Token = core.Token

}
