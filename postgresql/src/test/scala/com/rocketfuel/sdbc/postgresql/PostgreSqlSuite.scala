package com.rocketfuel.sdbc.postgresql

import java.time.OffsetDateTime
import com.rocketfuel.sdbc.config.TestingConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest._
import com.rocketfuel.sdbc.PostgreSql._
import fs2.Strategy

abstract class PostgreSqlSuite
  extends fixture.FunSuite
  with HasPostgreSqlPool
  with TestingConfig
  with PgTestingConfig
  with BeforeAndAfterAll {

  override def config: Config = ConfigFactory.load("sql-testing.conf")

  override def pgConfigKey: String = "pg"

  def testSelect[T](query: String, expectedValue: Option[T])(implicit converter: RowConverter[Option[T]]): Unit = {
    test(query) { implicit connection =>
      val result = Select[Option[T]](query).option().get
      (expectedValue, result) match {
        case (Some(expectedOffset: OffsetDateTime), Some(resultOffset: OffsetDateTime)) =>
          assertResult(expectedOffset.toInstant)(resultOffset.toInstant)
        case (expected, actual) =>
          assertResult(expected)(actual)
      }
    }
  }

  type FixtureParam = Connection

  override protected def withFixture(test: OneArgTest): Outcome = {
    withPg[Outcome] { connection =>
      withFixture(test.toNoArgTest(connection))
    }
  }

  override protected def beforeAll(): Unit = {
    pgBeforeAll()
    createLTree()
    createHstore()
  }

  override protected def afterAll(): Unit = {
    pgAfterAll()
  }

  implicit val strategy =
    Strategy.sequential

}
