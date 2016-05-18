package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.PostgreSql._
import com.rocketfuel.sdbc.config.TestingConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class HasPostgreSqlPoolSpec
  extends FunSuite
  with HasPostgreSqlPool
  with TestingConfig
  with PgTestingConfig
  with BeforeAndAfterAll {

  override def config: Config = ConfigFactory.load("sql-testing.conf")

  override def pgConfigKey: String = "pg"

  def testDatabaseExists(): Boolean = {
    withPgMaster[Boolean] { implicit connection =>
      Select[Singleton[Boolean]]("SELECT EXISTS(SELECT * FROM pg_database WHERE datname = @databaseName)").
        on("databaseName" -> pgTestCatalogName).run().get
    }
  }

  test("creates and destroys test database") {

    pgBeforeAll()

    assert(testDatabaseExists())

    pgDropTestCatalogs()

    assert(! testDatabaseExists())

  }

  override protected def afterAll(): Unit = {
    pgMasterPool.close()
  }
}
