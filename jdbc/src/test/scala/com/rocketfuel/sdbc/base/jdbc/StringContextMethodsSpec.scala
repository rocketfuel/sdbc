package com.rocketfuel.sdbc.base.jdbc

import TestDbms._
import org.scalatest.FunSuite

class StringContextMethodsSpec
  extends FunSuite {

  test("select") {
    assertCompiles("""select""""")
  }

  test("update") {
    assertCompiles("""update""""")
  }

  test("execute") {
    assertCompiles("""execute""""")
  }

  test("selectForUpdate") {
    assertCompiles("""selectForUpdate""""")
  }

}
