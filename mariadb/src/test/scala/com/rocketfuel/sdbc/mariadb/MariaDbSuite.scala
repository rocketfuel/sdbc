package com.rocketfuel.sdbc.mariadb

import com.rocketfuel.sdbc.MariaDb._
import org.scalatest._

abstract class MariaDbSuite
  extends fixture.FunSuite
  with BeforeAndAfterAll
  with HasMariaDbPool {

  type FixtureParam = Connection

  override protected def withFixture(test: OneArgTest): Outcome = {
    mariaPool.get.withConnection { connection: Connection =>
      withFixture(test.toNoArgTest(connection))
    }
  }

  override protected def beforeAll(): Unit = {
    mariaStart()
  }

  override protected def afterAll(): Unit = {
    mariaStop()
  }

}
