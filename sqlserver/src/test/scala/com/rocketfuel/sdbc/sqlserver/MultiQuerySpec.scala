package com.rocketfuel.sdbc.sqlserver

import com.rocketfuel.sdbc.SqlServer._
import shapeless._

class MultiQuerySpec
  extends SqlServerSuite {

  test("multiselect") {implicit connection =>

    val (results0, results1) =
      MultiQuery[(Vector[Int], Vector[Int])]("SELECT * FROM (VALUES (1)) AS MyTable(a); SELECT * FROM (VALUES (1)) AS MyTable2(b)").run()

    assertResult(Vector(1))(results0)
    assertResult(Vector(1))(results1)
  }

}
