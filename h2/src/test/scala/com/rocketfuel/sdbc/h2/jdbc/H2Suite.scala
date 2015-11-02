package com.rocketfuel.sdbc.h2.jdbc

import org.scalatest._
import H2._

abstract class H2Suite
  extends fixture.FunSuite {

  type FixtureParam = Connection

  override protected def withFixture(test: OneArgTest): Outcome = {
    withMemConnection[Outcome](name = "test", dbCloseDelay = Some(0)) { connection: Connection =>
      withFixture(test.toNoArgTest(connection))
    }
  }
}
