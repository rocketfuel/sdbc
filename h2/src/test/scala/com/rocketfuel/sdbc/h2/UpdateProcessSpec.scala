package com.rocketfuel.sdbc.h2

import scala.concurrent.duration._
import scalaz.stream._

class UpdateProcessSpec
  extends JdbcProcessSuite {

  test("Use a stream of Updates to insert rows using a connection pool.") { implicit connection =>

    val insertCount = inserts.toSource.through(Process.jdbc.keys.update(pool)).runLog.runFor(5.seconds).sum

    assertResult(expectedCount, "The expected number of rows were not inserted.")(insertCount)

  }

}
