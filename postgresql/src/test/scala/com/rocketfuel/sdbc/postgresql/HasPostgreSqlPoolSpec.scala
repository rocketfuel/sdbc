package com.rocketfuel.sdbc.postgresql

import org.scalatest._

class HasPostgreSqlPoolSpec
  extends FunSuite
    with HasPostgreSqlPool[PostgreSql]
    with BeforeAndAfterAll {

  override val postgresql: PostgreSql = com.rocketfuel.sdbc.PostgreSql
  import postgresql._

  def testDatabaseExists(): Boolean = {
    withPg[Boolean] { implicit connection =>
      Select[Boolean]("SELECT EXISTS(SELECT * FROM pg_database WHERE datname = @databaseName)").
        on("databaseName" -> dbName).one()
    }
  }

  test("creates test database") {
    pgStart()

    assert(testDatabaseExists())
  }

  test("connection unwrap works") {
    val connection = pgPool.get.getConnection()
    try assert(connection.escapeIdentifier("hi") != null)
    finally connection.close()
  }

  override protected def afterAll(): Unit = pgStop()

}
