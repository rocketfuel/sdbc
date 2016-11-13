package com.rocketfuel.sdbc.sqlserver

import com.rocketfuel.sdbc.SqlServer._

trait HasSqlServerPool {
  self: SqlTestingConfig =>

  val sqlTestCatalogName = sqlConfig.getString("catalog")

  protected var sqlPool: Option[Pool] = None

  protected lazy val sqlMasterPool: Pool = {
    val masterConfig = sqlConfig.toHikariConfig
    masterConfig.setMaximumPoolSize(1)
    masterConfig.setCatalog("master")

   new Pool(masterConfig)
  }

  protected def withSqlMaster[T](f: Connection => T): T = {
    val connection = sqlMasterPool.getConnection
    try {
      f(connection)
    } finally {
      if (!connection.isClosed) {
        connection.close()
      }
    }
  }

  protected def sqlCreateTestCatalog(): Unit = {
    if (sqlPool.isEmpty) {
      withSqlMaster { implicit connection =>
        Ignore(s"CREATE DATABASE [$sqlTestCatalogName];").ignore()
      }

      sqlPool = Some(Pool(sqlConfig))
    }
  }

  protected def sqlDropTestCatalogs(): Unit = {
    sqlPool.foreach(_.close())
    sqlPool = None

    withSqlMaster { implicit connection =>
      connection.setAutoCommit(true)

      val databases =
        Select[String]("SELECT name FROM sysdatabases WHERE name LIKE @catalogPrefix").
          on("catalogPrefix" -> (sqlTestCatalogPrefix + "%")).
          iterator()

      for (database <- databases) {
        util.Try {
          Ignore.ignore(
            s"""ALTER DATABASE [$database]
                |SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            """.stripMargin
          )

          Ignore.ignore(s"DROP DATABASE [$database]")
        }
      }
    }
  }

  def withSql[T](f: Connection => T): T = {
    val connection = sqlPool.get.getConnection
    try {
      f(connection)
    } finally {
      if (! connection.isClosed) {
        connection.close()
      }
    }
  }

  /**
   * Method for use with ScalaTest's beforeEach().
   */
  protected def sqlBeforeEach(): Unit = {
    sqlCreateTestCatalog()
  }

  /**
   * Method for use with ScalaTest's afterEach().
   */
  protected def sqlAfterEach(): Unit = {
    sqlDropTestCatalogs()
  }

  /**
   * Method for use with ScalaTest's beforeAll().
   */
  protected def sqlBeforeAll(): Unit = {
    sqlCreateTestCatalog()
  }

  /**
   * Method for use with ScalaTest's afterAll().
   */
  protected def sqlAfterAll(): Unit = {
    sqlDropTestCatalogs()
    sqlMasterPool.close()
  }

}
