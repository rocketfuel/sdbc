package com.rocketfuel.sdbc.sqlserver

import java.time.{Instant, OffsetDateTime}
import com.rocketfuel.sdbc.config.TestingConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest._

abstract class SqlServerSuite
  extends fixture.FunSuite
  with HasSqlServerPool
  with TestingConfig
  with SqlTestingConfig
  with BeforeAndAfterAll {

  override def config: Config = ConfigFactory.load("sql-testing.conf")

  override def sqlConfigKey: String = "sql"

  def testSelect[T](query: String, expectedValue: Option[T])(implicit converter: CompositeGetter[Option[T]]): Unit = {
    test(query) { implicit connection =>
      val result = Select[Option[T]](query).option().flatten
      (expectedValue, result) match {
        case (Some(expectedArray: Array[Byte]), Some(resultArray: Array[Byte])) =>
          assert(expectedArray.sameElements(resultArray))
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
    withSql[Outcome] { connection =>
      withFixture(test.toNoArgTest(connection))
    }
  }

  override protected def afterAll(): Unit = {
    sqlAfterAll()
  }

  override protected def beforeAll(): Unit = {
    sqlBeforeAll()
  }
}
