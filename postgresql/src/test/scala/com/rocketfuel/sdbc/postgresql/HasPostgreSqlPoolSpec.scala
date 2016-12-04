package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.PostgreSql._
import org.scalatest._

class HasPostgreSqlPoolSpec
  extends FunSuite
    with HasPostgreSqlPool
    with BeforeAndAfterAll {

  def testDatabaseExists(): Boolean = {
    withPg[Boolean] { implicit connection =>
      Select[Boolean]("SELECT EXISTS(SELECT * FROM pg_database WHERE datname = @databaseName)").
        on("databaseName" -> dbName).singleton()
    }
  }

  test("creates and destroys test database") {
    pgStart()

    assert(testDatabaseExists())
  }

  override protected def afterAll(): Unit = pgStop()

}
