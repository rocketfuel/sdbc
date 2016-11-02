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

}
