package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.CISet
import com.rocketfuel.sdbc.base.jdbc.resultset.DefaultGetters
import com.rocketfuel.sdbc.base.jdbc.statement.DefaultParameters

class TestDbms extends DBMS
  with DefaultParameters
  with DefaultGetters
  with DefaultUpdaters {

  override def dataSourceClassName: String = classOf[TestDataSource].getCanonicalName

  override def driverClassName: String = classOf[TestDriver].getCanonicalName

  override def supportsIsValid: Boolean = false

  override def productName: String = "test"

  override def jdbcSchemes: Set[String] = CISet("test")

}

object TestDbms extends TestDbms
