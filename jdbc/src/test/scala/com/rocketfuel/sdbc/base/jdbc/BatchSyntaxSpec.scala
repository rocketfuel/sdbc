package com.rocketfuel.sdbc.base.jdbc

import TestDbms.syntax._
import org.scalatest.FunSuite

class BatchSyntaxSpec extends FunSuite {

  test("batch() method on Seq works") {
    implicit def connection: TestDbms.Connection = ???

    assertCompiles("""Seq[TestDbms.CompiledParameterizedQuery[_]]().batches()""")
  }

}
