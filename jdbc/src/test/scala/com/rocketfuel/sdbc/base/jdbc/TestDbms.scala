package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.jdbc.resultset.DefaultGetters
import com.rocketfuel.sdbc.base.jdbc.statement.DefaultParameters

class TestDbms extends DBMS
  with DefaultParameters
  with DefaultGetters
  with DefaultUpdaters
  with JdbcConnection {

  override def productName: String = "test"

}

object TestDbms extends TestDbms
