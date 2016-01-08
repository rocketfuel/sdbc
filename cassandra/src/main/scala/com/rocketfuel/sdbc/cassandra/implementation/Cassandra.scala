package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core

private[sdbc] abstract class Cassandra
  extends ParameterValue
  with TupleParameterValues
  with TupleDataTypes
  with SessionMethods
  with Executable
  with Selectable
  with StringContextMethods
  with Select
  with Execute
  with CassandraProcess
  with Index
  with RowGetter
  with RowConverter
  with CompositeGetter {

  type Session = core.Session

  type Cluster = core.Cluster

  type UDTValue = core.UDTValue

  type TupleValue = core.TupleValue

  type Token = core.Token

}
