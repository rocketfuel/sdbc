package com.rocketfuel.sdbc.base.jdbc

import java.sql.{Connection => JConnection}

import com.typesafe.config.Config
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

case class Pool(configuration: HikariConfig) {

  val dbms: DBMS = DBMS.of(configuration)

  //Set the test query if the driver doesn't support .isValid().
  if (! dbms.supportsIsValid) {
    configuration.setConnectionTestQuery("SELECT 1")
  }

  val underlying = new HikariDataSource(configuration)

  def getConnection(): JConnection = {
    val connection = underlying.getConnection()
    dbms.initializeConnection(connection)
    connection
  }

  def withConnection[T](f: JConnection => T): T = {
    val connection = getConnection()
    try {
      f(connection)
    } finally {
      connection.close()
    }
  }

  def withTransaction[T](f: JConnection => T): T = {
    withConnection[T] { connection =>
      connection.setAutoCommit(false)
      val result = f(connection)
      connection.commit()
      result
    }
  }

}

object Pool {
  def apply(config: Config): Pool = {
    Pool(config.toHikariConfig)
  }
}

trait PoolImplicits {
  implicit def PoolToHikariDataSource(pool: Pool): HikariDataSource = {
    pool.underlying
  }
}
