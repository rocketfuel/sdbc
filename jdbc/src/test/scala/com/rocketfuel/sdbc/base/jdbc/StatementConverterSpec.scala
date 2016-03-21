package com.rocketfuel.sdbc.base.jdbc

import org.scalatest.FunSuite
import TestDbms._

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

  test("(Unit, UpdateCount)") {
    assertCompiles("StatementConverter[(Unit, UpdateCount)]")
  }

}
