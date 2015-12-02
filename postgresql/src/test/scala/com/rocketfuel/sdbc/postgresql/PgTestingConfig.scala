package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.config.TestingConfig
import com.typesafe.config.{Config, ConfigFactory}

trait PgTestingConfig {
  self: TestingConfig =>

  lazy val pgTestCatalogPrefix: String = config.getString("testCatalogPrefix")

  def pgConfigKey: String

  lazy val pgRandomCatalog =
    ConfigFactory.parseString("dataSource.databaseName = " + testCatalogName)

  lazy val pgConfig: Config =
    PgTestingConfig.defaults.
    withFallback(config.getConfig(pgConfigKey)).
    withFallback(pgRandomCatalog)

}

object PgTestingConfig {

  val defaults = {
    val asString =
      """autoCommit = false
        |dataSourceClassName = "org.postgresql.ds.PGSimpleDataSource"
      """.stripMargin

    ConfigFactory.parseString(asString)
  }

}
