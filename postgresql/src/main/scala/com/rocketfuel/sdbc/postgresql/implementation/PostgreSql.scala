package com.rocketfuel.sdbc.postgresql.implementation

import com.rocketfuel.sdbc.base.jdbc._

private[sdbc] abstract class PostgreSql
  extends DBMS
  with Setters
  with IntervalImplicits
  with Getters
  with Updaters
  with ArrayTypes
  with PgConnection {

  type Cidr = com.rocketfuel.sdbc.postgresql.Cidr

  val Cidr = com.rocketfuel.sdbc.postgresql.Cidr

  type LTree = com.rocketfuel.sdbc.postgresql.LTree

  val LTree = com.rocketfuel.sdbc.postgresql.LTree

}
