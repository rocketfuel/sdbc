package com.rocketfuel.sdbc.postgresql.implementation

import com.rocketfuel.sdbc.base.CISet
import com.rocketfuel.sdbc.base.jdbc._

private[sdbc] abstract class PostgreSql
  extends DBMS
  with Setters
  with IntervalImplicits
  with Getters
  with Updaters
  with ArrayTypes
  with PgConnection {

  override def dataSourceClassName = "org.postgresql.ds.PGSimpleDataSource"
  override def jdbcSchemes = CISet("postgresql")
  override def productName: String = "PostgreSQL"
  override val supportsIsValid: Boolean = true

}
