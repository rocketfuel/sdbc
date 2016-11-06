package com.rocketfuel.sdbc.base.jdbc

import com.typesafe.config.Config
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

trait Pool {
  self: DBMS with Connection =>

  class Pool(configuration: HikariConfig) {

    //Set the test query if the driver doesn't support .isValid().
    connectionTestQuery.foreach(configuration.setConnectionTestQuery)

    val underlying = new HikariDataSource(configuration)

    def getConnection(): Connection = {
      new Connection(underlying.getConnection())
    }

    def getConnection(username: String, password: String): Connection = {
      new Connection(underlying.getConnection(username, password))
    }

    def withConnection[T](f: Connection => T): T = {
      Connection.using(this)(f)
    }

    def withTransaction[T](f: Connection => T): T = {
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

    def apply(config: HikariConfig): Pool = {
      new Pool(config)
    }

    implicit def toHikariDataSource(pool: Pool): HikariDataSource = {
      pool.underlying
    }
  }

}
