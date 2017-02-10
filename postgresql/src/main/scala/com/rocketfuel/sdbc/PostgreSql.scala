package com.rocketfuel.sdbc

object PostgreSql
  extends postgresql.PostgreSql
    with postgresql.ArgonautSupport

object PostgreSqlJson4s
  extends postgresql.PostgreSql
    with postgresql.Json4sSupport
