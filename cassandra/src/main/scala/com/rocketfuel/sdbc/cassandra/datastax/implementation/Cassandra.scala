package com.rocketfuel.sdbc.cassandra.datastax.implementation

import com.rocketfuel.sdbc.base.ParameterValueImplicits
import com.rocketfuel.sdbc.cassandra.datastax.implementation

abstract class Cassandra
  extends RowGetters
  with RowGetterImplicits
  with IndexImplicits
  with RowMethods
  with ParameterValues
  with TupleParameterValues
  with TupleDataTypes
  with ParameterValueImplicits
  with TupleGetters
  with SessionMethods
  with ExecutableMethods
  with SelectableMethods
  with StringContextMethods {

  type ParameterValue[+T] = implementation.ParameterValue[T]

  type ParameterList = implementation.ParameterList

  type Session = implementation.Session

  type Cluster = implementation.Cluster

  type Executable[Key] = implementation.Executable[Key]

  type Selectable[Key, Value] = implementation.Selectable[Key, Value]

}
