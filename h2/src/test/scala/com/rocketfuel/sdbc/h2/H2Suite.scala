package com.rocketfuel.sdbc.h2

import com.rocketfuel.sdbc.H2._
import org.scalatest._

abstract class H2Suite
  extends fixture.FunSuite {

  type FixtureParam = Connection

  override protected def withFixture(test: OneArgTest): Outcome = {
    Connection.using("jdbc:h2:mem:test;DB_CLOSE_DELAY=0") { connection: Connection =>
      withFixture(test.toNoArgTest(connection))
    }
  }

}
