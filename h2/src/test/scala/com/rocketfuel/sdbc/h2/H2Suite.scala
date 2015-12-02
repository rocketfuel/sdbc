package com.rocketfuel.sdbc.h2

import com.rocketfuel.sdbc.h2.H2._
import org.scalatest._

abstract class H2Suite
  extends fixture.FunSuite {

  type FixtureParam = Connection

  override protected def withFixture(test: OneArgTest): Outcome = {
    withMemConnection[Outcome](name = "test", dbCloseDelay = Some(0)) { connection: Connection =>
      withFixture(test.toNoArgTest(connection))
    }
  }
}
