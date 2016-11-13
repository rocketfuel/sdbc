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

    /**
      * Run some method with a connection from this pool. The connection is automatically
      * closed.
      *
      * Do not return the connection, as it will have been closed.
      */
    def withConnection[T](f: Connection => T): T = {
      Connection.using(this)(f)
    }

    /**
      * Run some method with a connection from this pool. The connection is automatically
      * closed.
      *
      * Do not return the connection, as it will have been closed.
      */
    def withConnection[T](username: String, password: String)(f: Connection => T): T = {
      Connection.using(this, username, password)(f)
    }

    /**
      * Sets autoCommit to false, runs your action, and then runs commit() before closing the connection.
      *
      * Do not return the connection, as it will have been closed.
      */
    def withTransaction[T](f: Connection => T): T = {
      Connection.usingTransaction(this)(f)
    }

    /**
      * Sets autoCommit to false, runs your action, and then runs commit() before closing the connection.
      *
      * Do not return the connection, as it will have been closed.
      */
    def withTransaction[T](username: String, password: String)(f: Connection => T): T = {
      Connection.usingTransaction(this, username, password)(f)
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
