package com.wda.sdbc.jdbc.scalaz

import scalaz.stream._
import scala.concurrent.duration._

class SelectProcessSpec
  extends JdbcProcessSuite {

  test("Use a stream of Select to select rows using a connection pool.") { implicit connection =>

    val selectFuture = for {
      _ <- inserts.toSource.through(Process.jdbc.keys.update).run
      rows <- Process.jdbc.select(select).runLog
    } yield rows

    val selectResults = selectFuture.runFor(5.seconds)

    assertResult(expectedCount)(selectResults.size.toLong)

    assertResult(insertSet)(selectResults.toSet)

  }

}
