package com.rocketfuel.sdbc.base.jdbc

import org.scalatest.FunSuite
import TestDbms._
import java.sql.ResultSet

class StatementConverterSpec
  extends FunSuite
  with TypesForTesting {

  test("Unit") {
    assertCompiles("StatementConverter[Unit]")
  }

  test("UpdateCount") {
    assertCompiles("StatementConverter[UpdateCount]")
  }

  test("Iterator[UpdatableRow]") {
    assertCompiles("StatementConverter[Iterator[UpdatableRow]]")
  }

  test("Iterator[Iterator[UpdatableRow]]") {
    assertDoesNotCompile("StatementConverter[Iterator[Iterator[UpdatableRow]]]")
  }

  test("StatementConverter[Iterator[Int]]") {
    assertCompiles("StatementConverter[Iterator[Int]]")
  }

  test("StatementConverter[Vector[Int]]") {
    assertCompiles("StatementConverter[Vector[Int]]")
  }

  test("StatementConverter[Vector[Seq[Int]]]") {
    assertCompiles("StatementConverter[Vector[Seq[Int]]]")
  }

  test("StatementConverter[Vector[Seq[Seq[Int]]]]") {
    assertCompiles("StatementConverter[Vector[Seq[Seq[Int]]]]")
  }

  test("ResultSet") {
    assertCompiles("StatementConverter[ResultSet]")
  }

  test("(Unit, UpdateCount, ResultSet)") {
    assertCompiles("StatementConverter[(Unit, UpdateCount, ResultSet)]")
  }

}
