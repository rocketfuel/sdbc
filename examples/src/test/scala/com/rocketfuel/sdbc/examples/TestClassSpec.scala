package com.rocketfuel.sdbc.examples

import com.rocketfuel.sdbc.h2.{H2Suite, H2}
import H2._

class TestClassSpec
 extends H2Suite {

  test("insert and select works") { implicit connection =>
    connection.execute("CREATE TEMPORARY TABLE test_class (id int IDENTITY(1,1) PRIMARY KEY, value varchar(100) NOT NULL)")
    assertResult(1)(update(TestClass.Value("hi")))
    val rows = iterator(TestClass.All).toSeq
    assert(rows.exists(_.value == "hi"))
  }

}
