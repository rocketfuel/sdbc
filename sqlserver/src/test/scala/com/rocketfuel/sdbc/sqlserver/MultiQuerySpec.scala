package com.rocketfuel.sdbc.sqlserver

import com.rocketfuel.sdbc.SqlServer._
import shapeless._

class MultiQuerySpec
  extends SqlServerSuite {

  test("manual") {implicit connection =>
    val s = connection.prepareStatement("SELECT * FROM (VALUES (1)) AS MyTable(a); SELECT * FROM (VALUES (2)) AS MyTable2(b)")
    s.execute()
    val r0 = s.getResultSet()
    s.getMoreResults(java.sql.Statement.KEEP_CURRENT_RESULT)
    val r1 = s.getResultSet()
    r0.next()
    println(s"val is ${r0.getInt(1)}")
    r1.next()
    println(s"val is ${r1.getInt(1)}")
  }

  test("multiselect") {implicit connection =>

    val (results0, results1) =
      MultiQuery[(QueryResult.Vector[Int], QueryResult.Vector[Int])]("SELECT * FROM (VALUES (1)) AS MyTable(a); SELECT * FROM (VALUES (2)) AS MyTable2(b)").run()

    assertResult(Vector(1))(results0.get)
    assertResult(Vector(2))(results1.get)
  }

}
