package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.PostgreSql._
import com.zaxxer.hikari.HikariConfig
import ru.yandex.qatools.embed.postgresql._
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig

trait HasPostgreSqlPool {

  val dbName = "postgres"
  val user = "postgres"
  val password = "postgres"

  var pgProcess: Option[PostgresProcess] = None
  var pgPool: Option[Pool] = None

  val pgServer = PostgresStarter.getDefaultInstance()
  val pgConfig = PostgresConfig.defaultWithDbName(dbName, user, password)

  def pgStartServer(): Unit = {
    pgProcess = Some(pgServer.prepare(pgConfig).start())
  }

  def startPool(): Unit = {
    val poolConfig = new HikariConfig()
    poolConfig.setUsername(user)
    poolConfig.setPassword(password)
    poolConfig.setCatalog(dbName)
    poolConfig.setDataSourceClassName(classOf[org.postgresql.ds.PGSimpleDataSource].getCanonicalName)
    poolConfig.getDataSourceProperties.setProperty("PortNumber", pgConfig.net.port.toString)
    poolConfig.setMaximumPoolSize(10)

    pgPool = Some(new Pool(poolConfig))
  }

  protected def withPg[T](f: Connection => T): T = {
    val connection = pgPool.get.getConnection()
    try {
      f(connection)
    } finally {
      connection.close()
    }
  }

  def createHstore(): Unit = {
    withPg {implicit connection =>
      ignore"CREATE EXTENSION hstore".ignore()
    }
  }

  def createLTree(): Unit = {
    withPg { implicit connection =>
      Ignore("CREATE EXTENSION ltree;").ignore()
    }
  }

  /**
   * Method for use with ScalaTest's beforeEach().
   */
  def pgStart(): Unit = {
    pgStartServer()
    startPool()
  }

  /**
   * Method for use with ScalaTest's afterAll().
   */
  def pgStop(): Unit = {
    pgPool.foreach(_.close())
    pgPool = None
    pgProcess.foreach(_.stop())
    pgProcess = None
  }
}
