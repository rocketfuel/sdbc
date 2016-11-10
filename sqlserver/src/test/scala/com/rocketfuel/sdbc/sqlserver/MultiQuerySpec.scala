package com.rocketfuel.sdbc.sqlserver

import com.rocketfuel.sdbc.SqlServer._
import shapeless._

class MultiQuerySpec
  extends SqlServerSuite {

  test("vector vector") {implicit connection =>
    val (results0, results1) =
      MultiQuery[(QueryResult.Vector[Int], QueryResult.Vector[Int])]("SELECT * FROM (VALUES (1)) AS MyTable(a); SELECT * FROM (VALUES (2)) AS MyTable2(b)").run()

    assertResult(Vector(1))(results0.get)
    assertResult(Vector(2))(results1.get)
  }

  test("update vector") {implicit connection =>
    connection.setAutoCommit(true)
    val tbl = util.Random.nextString(10)
    Execute.execute(s"CREATE TABLE [$tbl] (i int)")

    val (results0, results1) =
      MultiQuery[(QueryResult.Update, QueryResult.Vector[Int])](s"INSERT INTO [$tbl](i) VALUES (1); SELECT i FROM [$tbl];").run()

    assertResult(1)(results0.get)
    assertResult(Vector(1))(results1.get)
  }

}
