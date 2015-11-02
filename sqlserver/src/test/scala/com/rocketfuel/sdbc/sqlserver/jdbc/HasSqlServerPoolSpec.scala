package com.rocketfuel.sdbc.sqlserver.jdbc

import com.typesafe.config.{ConfigFactory, Config}
import com.rocketfuel.sdbc.config.TestingConfig
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import SqlServer._

class HasSqlServerPoolSpec
  extends FunSuite
  with HasSqlServerPool
  with TestingConfig
  with SqlTestingConfig
  with BeforeAndAfterAll {

  override def sqlConfigKey: String = "sql"

  override def config: Config = ConfigFactory.load("sql-testing.conf")

  def testDatabaseExists(): Boolean = {
    withSqlMaster[Boolean] { implicit connection =>
      Select[Int]("SELECT CASE WHEN db_id(@databaseName) IS NULL THEN 0 ELSE 1 END").on("databaseName" -> sqlTestCatalogName).option().exists(_ == 1)
    }
  }

  test("creates and destroys test database") {

    sqlBeforeAll()

    assert(testDatabaseExists())

    sqlDropTestCatalogs()

    assert(! testDatabaseExists())

  }

  override protected def afterAll(): Unit = {
    sqlMasterPool.close()
  }
}

