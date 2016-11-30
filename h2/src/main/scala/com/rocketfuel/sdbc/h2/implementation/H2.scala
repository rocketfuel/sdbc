package com.rocketfuel.sdbc.h2.implementation

import com.rocketfuel.sdbc.base.jdbc.resultset.{DefaultGetters, SeqGetter}
import com.rocketfuel.sdbc.base.jdbc.statement.{DefaultParameters, SeqParameter}
import com.rocketfuel.sdbc.base.jdbc
import com.rocketfuel.sdbc.h2

private[sdbc] abstract class H2
  extends jdbc.DBMS
  with DefaultGetters
  with DefaultParameters
  with jdbc.DefaultUpdaters
  with SeqParameter
  with SeqGetter
  with ArrayTypes
  with SerializedParameter
  with jdbc.JdbcConnection {

  type Serialized = h2.Serialized
  val Serialized = h2.Serialized

}
