package com.rocketfuel.sdbc

object PostgreSql
  extends postgresql.PostgreSql {
  override def initializeJson(connection: PostgreSql.Connection): Unit = ()
}

object PostgreSqlArgonaut
  extends postgresql.PostgreSql
    with postgresql.ArgonautSupport

object PostgreSqlJson4SJackson
  extends postgresql.PostgreSql
    with postgresql.Json4sJacksonSupport

object PostgreSqlJson4SNative
  extends postgresql.PostgreSql
    with postgresql.Json4sNativeSupport

object PostgreSqlCirce
  extends postgresql.PostgreSql
    with postgresql.CirceSupport
