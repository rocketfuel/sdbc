package com.rocketfuel.sdbc.sqlserver

import com.rocketfuel.sdbc.SqlServer._
import shapeless._

class MultiQuerySpec
  extends SqlServerSuite {

  test("vector vector") {implicit connection =>
    val (results0, results1) =
      MultiQuery.run[(QueryResult.Vector[Int], QueryResult.Vector[Int])]("SELECT * FROM (VALUES (1)) AS MyTable(a); SELECT * FROM (VALUES (2)) AS MyTable2(b)")

    assertResult(Vector(1))(results0.get)
    assertResult(Vector(2))(results1.get)
  }

  test("update vector") {implicit connection =>
    val tbl = util.Random.nextString(10)
    Execute.execute(s"CREATE TABLE [$tbl] (i int PRIMARY KEY)")

    val (results0, results1) =
      MultiQuery.run[(QueryResult.Update, QueryResult.Vector[Int])](s"INSERT INTO [$tbl](i) VALUES (1); SELECT i FROM [$tbl];")

    assertResult(1)(results0.get)
    assertResult(Vector(1))(results1.get)
  }

  test("iterator vector") {implicit connection =>
    val tbl = util.Random.nextString(10)
    Execute.execute(s"CREATE TABLE [$tbl] (i int PRIMARY KEY)")
    Execute.execute(s"INSERT INTO [$tbl](i) VALUES (1)")

    val (results0, results1) =
      MultiQuery.run[(QueryResult.Iterator[Int], QueryResult.Vector[Int])](s"SELECT i FROM [$tbl]; SELECT i FROM [$tbl];")

    assertResult(Vector(1))(results0.get.toVector)
    assertResult(Vector(1))(results1.get)
  }

}
