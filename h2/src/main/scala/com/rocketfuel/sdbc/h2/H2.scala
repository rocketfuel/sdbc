package com.rocketfuel.sdbc.h2

import com.rocketfuel.sdbc.base.jdbc
import com.rocketfuel.sdbc.base.jdbc.resultset.{DefaultGetters, SeqGetter}
import com.rocketfuel.sdbc.base.jdbc.statement.{DefaultParameters, SeqParameter}

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

}
