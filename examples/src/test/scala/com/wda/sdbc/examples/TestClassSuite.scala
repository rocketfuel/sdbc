package com.wda.sdbc.examples

import com.wda.sdbc.H2._
import com.wda.sdbc.h2.H2Suite

class TestClassSuite
 extends H2Suite {

  test("insert and select works") { implicit connection =>
    connection.execute("CREATE TEMPORARY TABLE test_class (id int IDENTITY(1,1) PRIMARY KEY, value varchar(100) NOT NULL)")
    assertResult(1)(TestClass.update(TestClass.Value("hi")))
    val rows = TestClass.iterator(TestClass.All()).toSeq
    assert(rows.exists(_.value == "hi"))
  }

}
