package com.rocketfuel.sdbc.h2

import com.rocketfuel.sdbc.H2._
import com.rocketfuel.sdbc.H2.syntax._

class BatchableSpec
  extends H2Suite {

  implicit val s: Insertable[Int] =
    Insert("INSERT INTO tbl (i) VALUES (@i)").insertable[Int].parameters(i => Parameters("i" -> i))

  test("batch of partable works") {implicit connection =>
    Ignore.ignore("CREATE TABLE tbl (i int)")
    val keys =
      Seq(1, 2, 3)

    val result =
      keys.batches()

    assertResult(3)(result.sum)
  }

}
