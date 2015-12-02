package com.rocketfuel.sdbc.postgresql.jdbc

import java.time.{Instant, OffsetDateTime}
import com.rocketfuel.sdbc.PostgreSql
import com.typesafe.config.{ConfigFactory, Config}
import com.rocketfuel.sdbc.config.TestingConfig
import org.scalatest._
import PostgreSql._

abstract class PostgreSqlSuite
  extends fixture.FunSuite
  with HasPostgreSqlPool
  with TestingConfig
  with PgTestingConfig
  with BeforeAndAfterAll {

  override def config: Config = ConfigFactory.load("sql-testing.conf")

  override def pgConfigKey: String = "pg"

  def testSelect[T](query: String, expectedValue: Option[T])(implicit converter: CompositeGetter[Option[T]]): Unit = {
    test(query) { implicit connection =>
      val result = Select[Option[T]](query).option().get
      (expectedValue, result) match {
        case (Some(expectedOffset: OffsetDateTime), Some(resultOffset: OffsetDateTime)) =>
          assertResult(expectedOffset.toInstant)(resultOffset.toInstant)
        case (Some(expectedInstant: Instant), Some(resultInstant: Instant)) =>
          assertResult(expectedInstant)(resultInstant)
        case (Some(x), Some(y)) =>
          assertResult(x)(y)
        case (None, None) => true
        case _ => false
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
}
