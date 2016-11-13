package com.rocketfuel.sdbc.postgresql

import org.scalatest.FunSuite

class LTreeSpec extends FunSuite {

  test("empty LTrees are allowed") {
    assertResult(Vector.empty[String])(LTree().getPath)
  }

  test("empty LTree nodes are not allowed") {
    intercept[IllegalArgumentException](LTree(""))
  }

  test("Pattern matching works.") {
    assertCompiles("""val LTree(v0, v1) = LTree("hi", "there")""")
  }

}
