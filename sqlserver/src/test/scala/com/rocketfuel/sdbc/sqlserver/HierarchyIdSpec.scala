package com.rocketfuel.sdbc.sqlserver

import org.scalatest.FunSuite

class HierarchyIdSpec extends FunSuite {

  test("empty path from string") {
    assertResult(HierarchyId.empty)(HierarchyId.fromString("/"))
  }

  test("empty path to string") {
    assertResult("/")(HierarchyId().toString)
  }

  test("one node from string") {
    assertResult(HierarchyId(1))(HierarchyId.fromString("/1/"))
  }

  test("one node to string") {
    assertResult("/1/")(HierarchyId(1).toString)
  }

  test("one node next to a node from string") {
    assertResult(HierarchyId(Seq(1, 1)))(HierarchyId.fromString("/1.1/"))
  }

  test("one node next to a node to string") {
    assertResult("/1.1/")(HierarchyId(Seq(1, 1)).toString)
  }

}
