package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.jdbc.resultset.DefaultGetters
import com.rocketfuel.sdbc.base.jdbc.statement.DefaultParameters

class TestDbms extends DBMS
  with DefaultParameters
  with DefaultGetters
  with DefaultUpdaters
  with JdbcConnection
  with MultiQuery {

  trait syntax
    extends super.syntax
      with MultiQueryable.Partable
      with MultiQueryable.syntax

  override val syntax = new syntax {}

}

object TestDbms extends TestDbms
