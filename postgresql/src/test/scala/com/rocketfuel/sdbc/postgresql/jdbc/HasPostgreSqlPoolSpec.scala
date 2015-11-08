package com.rocketfuel.sdbc.postgresql.jdbc

import com.typesafe.config.{ConfigFactory, Config}
import com.rocketfuel.sdbc.config.TestingConfig
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import PostgreSql._

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
      Select[Boolean]("SELECT EXISTS(SELECT * FROM pg_database WHERE datname = @databaseName)").on("databaseName" -> pgTestCatalogName).option().get
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
