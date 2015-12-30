package com.rocketfuel.sdbc.h2

import scala.concurrent.duration._
import scalaz.stream._
import com.rocketfuel.sdbc.H2._

class SelectProcessSpec
  extends JdbcProcessSuite {

  test("Use a stream of Select to select rows using a connection pool.") { implicit connection =>

    val selectFuture = for {
      _ <- inserts.toSource.through(Process.jdbc.keys.update(pool)).run
      rows <- Process.jdbc.select(select).runLog
    } yield rows

    val selectResults = selectFuture.runFor(5.seconds)

    assertResult(expectedCount)(selectResults.size.toLong)

    assertResult(insertSet)(selectResults.toSet)

  }

}
