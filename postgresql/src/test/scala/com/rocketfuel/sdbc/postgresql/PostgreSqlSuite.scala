package com.rocketfuel.sdbc.postgresql

import java.time.OffsetDateTime
import org.scalatest._
import org.apache.commons.lang3.RandomStringUtils
import scala.reflect.ClassTag

abstract class PostgreSqlSuite[P <: PostgreSql]
  extends fixture.FunSuite
  with HasPostgreSqlPool[P]
  with BeforeAndAfterAll {

  import postgresql._

  def testSelect[T](query: String, expectedValue: Option[T])(implicit converter: RowConverter[Option[T]]): Unit = {
    test(query) { implicit connection =>
      val result = Select[Option[T]](query).one()
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
    pgStart()
    createLTree()
    createHstore()
  }

  override protected def afterAll(): Unit = {
    pgStop()
  }

  def testUpdate[T](
    typeName: String
  )(before: T
  )(after: T
  )(implicit ctag: ClassTag[T],
    updater: Updater[T],
    setter: T => ParameterValue,
    converter: RowConverter[Option[T]]
  ): Unit = {
    test(s"Update ${ctag.runtimeClass.getName}") {implicit connection =>
      val tableName = RandomStringUtils.randomAlphabetic(10)

      Ignore.ignore(s"CREATE TABLE $tableName (id serial PRIMARY KEY, v $typeName)")

      Ignore.ignore(s"INSERT INTO $tableName (v) VALUES (@before :: $typeName)", Map("before" -> before))

      def updateRow(row: UpdatableRow): Unit = {
        row("v") = after
        row.updateRow()
      }

      val summary =  SelectForUpdate.update(s"SELECT * FROM $tableName", rowUpdater = updateRow)

      assertResult(UpdatableRow.Summary(updatedRows = 1))(summary)

      val maybeValue = Select.one[Option[T]](s"SELECT v FROM $tableName")

      assert(maybeValue.nonEmpty)

      (after, maybeValue.get) match {
        case (a: Array[_], b: Array[_]) =>
          assert(a.sameElements(b))
        case (expectedAfter, actualAfter) =>
          assertResult(expectedAfter)(actualAfter)
      }
    }
  }

}

object PostgreSqlSuite {

  abstract class Base
    extends {
      override val postgresql: PostgreSql = new PostgreSql {
        override def initializeJson(connection: Connection): Unit = ()
      }
    } with PostgreSqlSuite[PostgreSql]

  abstract class Argonaut
    extends {
    override val postgresql: com.rocketfuel.sdbc.PostgreSqlArgonaut.type = com.rocketfuel.sdbc.PostgreSqlArgonaut
  } with PostgreSqlSuite[com.rocketfuel.sdbc.PostgreSqlArgonaut.type]

  abstract class Json4sJackson
    extends {
      override val postgresql: com.rocketfuel.sdbc.PostgreSqlJson4SJackson.type = com.rocketfuel.sdbc.PostgreSqlJson4SJackson
    } with PostgreSqlSuite[com.rocketfuel.sdbc.PostgreSqlJson4SJackson.type]

  abstract class Json4sNative
    extends {
      override val postgresql: com.rocketfuel.sdbc.PostgreSqlJson4SNative.type = com.rocketfuel.sdbc.PostgreSqlJson4SNative
    } with PostgreSqlSuite[com.rocketfuel.sdbc.PostgreSqlJson4SNative.type]

  abstract class Circe
    extends {
      override val postgresql: com.rocketfuel.sdbc.PostgreSqlCirce.type = com.rocketfuel.sdbc.PostgreSqlCirce
    } with PostgreSqlSuite[com.rocketfuel.sdbc.PostgreSqlCirce.type]

}
