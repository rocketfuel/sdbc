package com.example

import com.rocketfuel.sdbc.H2
import org.scalatest.{BeforeAndAfterEach, _}

class TestClassSpec
 extends fixture.FunSuite
 with BeforeAndAfterEach {

  type FixtureParam = H2.Connection

  private val connectionString = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1"

  override protected def withFixture(test: OneArgTest): Outcome = {
    H2.Connection.using(connectionString) { connection: H2.Connection =>
      withFixture(test.toNoArgTest(connection))
    }
  }

  val before = "hi"

  val after = "bye"

  test("insert and select works") {implicit connection =>
    assertResult(1)(H2.Updatable.update(TestClass.Value(before)))
    val rows = H2.Selectable.vector(TestClass.All)
    assert(rows.exists(_.value == before), "The row wasn't inserted.")
  }

  test("select for update works") {implicit connection =>
    //insert a row
    assertResult(1)(H2.Updatable.update(TestClass.Value(before)))

    //update all the values to "bye"
    val summary = H2.SelectForUpdatable.update(TestClass.All(after))
    assertResult(H2.UpdatableRow.Summary(updatedRows = 1))(summary)

    //Make sure "hi" disappeared and "bye" exists.
    val resultRows = H2.Selectable.vector(TestClass.All)
    assert(resultRows.forall(_.value == after), "The value wasn't updated.")
  }

  override protected def beforeEach(): Unit = {
    H2.Connection.using(connectionString) {implicit connection =>
      H2.Ignore.ignore("CREATE TABLE test_class (id int IDENTITY(1,1) PRIMARY KEY, value varchar(100) NOT NULL)")
    }
  }

  override protected def afterEach(): Unit = {
    H2.Connection.using(connectionString) {implicit connection =>
      H2.Ignore.ignore("DROP TABLE test_class")
    }
  }
}
