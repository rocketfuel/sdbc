package com.rocketfuel.sdbc.h2

import com.zaxxer.hikari.HikariConfig
import org.scalatest.{Outcome, fixture, BeforeAndAfterEach}
import scalaz.stream._
import com.rocketfuel.sdbc.H2._

abstract class JdbcProcessSuite
  extends fixture.FunSuite
  with BeforeAndAfterEach {
  suite =>

  type FixtureParam = Connection

  override protected def withFixture(test: OneArgTest): Outcome = {
    pool.withConnection[Outcome]{connection => withFixture(test.toNoArgTest(connection))}
  }

  implicit val pool = {
    val poolConfig = new HikariConfig()
    poolConfig.setJdbcUrl("jdbc:h2:mem:stream_test;DB_CLOSE_DELAY=-1")

    Pool(poolConfig)
  }

  val expectedCount = 100L

  case class LongKey(key: Long)

  implicit val LongInsertable = new QueryForUpdatable[LongKey] {
    val update = QueryForUpdate("INSERT INTO tbl (i) VALUES (@key)")

    override def update(key: LongKey): QueryForUpdate = {
      update.onProduct(key)
    }
  }

  val insertSet = 0L.until(expectedCount).toSet

  val inserts =
    Process.emitAll(insertSet.toSeq.map(key => LongKey(key)))

  val select = Query[Long]("SELECT i FROM tbl")

  override protected def beforeEach(): Unit = {
    pool.withConnection[Unit] {implicit connection =>
      Execute("CREATE TABLE tbl (i bigint PRIMARY KEY)").execute()
    }
  }

  override protected def afterEach(): Unit = {
    pool.withConnection[Unit] {implicit connection =>
      Execute("DROP TABLE tbl").execute()
    }
  }

}
