package com.rocketfuel.sdbc

object PostgreSql
  extends postgresql.PostgreSql {
  override def initializeJson(connection: PostgreSql.Connection): Unit = ()
}

object PostgreSqlArgonaut
  extends postgresql.PostgreSql
    with postgresql.ArgonautSupport

object PostgreSqlJson4s
  extends postgresql.PostgreSql
    with postgresql.Json4sSupport

object PostgreSqlCirce
  extends postgresql.PostgreSql
    with postgresql.CirceSupport
