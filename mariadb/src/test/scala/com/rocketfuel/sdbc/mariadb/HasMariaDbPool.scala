package com.rocketfuel.sdbc.mariadb

import ch.vorburger.mariadb4j.DB
import com.rocketfuel.sdbc.MariaDb._
import com.zaxxer.hikari.HikariConfig

trait HasMariaDbPool {

  val dbName = "test"
  val user = "root"
  val password = ""

  var mariaProcess: Option[DB] = None
  var mariaPool: Option[Pool] = None

  def mariaStartServer(): Unit = {
    mariaProcess = Some {
      val server = DB.newEmbeddedDB(0)
      server.start()
      server
    }
  }

  def startPool(): Unit = {
    val poolConfig = new HikariConfig()
    poolConfig.setJdbcUrl(s"jdbc:mysql://localhost:${mariaProcess.get.getConfiguration.getPort}/$dbName?allowMultiQueries=true")
    poolConfig.setUsername(user)
    poolConfig.setPassword(password)
    poolConfig.setMaximumPoolSize(10)

    mariaPool = Some(new Pool(poolConfig))
  }

  protected def withMaria[T](f: Connection => T): T = {
    val connection = mariaPool.get.getConnection()
    try {
      f(connection)
    } finally {
      connection.close()
    }
  }

  /**
    * Method for use with ScalaTest's beforeEach().
    */
  def mariaStart(): Unit = {
    mariaStartServer()
    startPool()
  }

  /**
    * Method for use with ScalaTest's afterAll().
    */
  def mariaStop(): Unit = {
    mariaPool.foreach(_.close())
    mariaPool = None
    mariaProcess.foreach(_.stop())
    mariaProcess = None
  }

}
