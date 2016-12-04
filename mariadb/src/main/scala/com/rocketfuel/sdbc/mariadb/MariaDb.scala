package com.rocketfuel.sdbc.mariadb

import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.base.jdbc.resultset.DefaultGetters
import com.rocketfuel.sdbc.base.jdbc.statement.DefaultParameters

trait MariaDb
  extends DBMS
    with DefaultGetters
    with DefaultParameters
    with DefaultUpdaters
    with JdbcConnection
