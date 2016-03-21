package com.rocketfuel.sdbc.base.jdbc

import org.scalatest.FunSuite

class GetterSpec extends FunSuite {

  test("Int") {
    assertCompiles("TestDbms.Getter[TestDbms.Row, Int]")
  }

  test("Seq[Int]") {
    assertCompiles("TestDbms.Getter[TestDbms.Row, Seq[Int]]")
  }

  test("Seq[Byte] uses SeqByteGetter") {
    val implicitSeqByteGetter = implicitly[TestDbms.Getter[TestDbms.Row, Seq[Byte]]]

    assertResult(TestDbms.SeqByteGetter)(implicitSeqByteGetter)
  }

  test("InputStreams can only be gotten from UpdatableRows") {
    import java.io.InputStream
    assertDoesNotCompile("TestDbms.Getter[TestDbms.ImmutableRow, InputStream]")
    assertDoesNotCompile("TestDbms.Getter[TestDbms.ImmutableRow, InputStream]")
    assertCompiles("TestDbms.Getter[TestDbms.UpdatableRow, InputStream]")
  }

  test("Readers can only be gotten from UpdatableRows") {
    import java.io.Reader
    assertDoesNotCompile("TestDbms.Getter[TestDbms.ImmutableRow, Reader]")
    assertDoesNotCompile("TestDbms.Getter[TestDbms.ImmutableRow, Reader]")
    assertCompiles("TestDbms.Getter[TestDbms.UpdatableRow, Reader]")
  }

}
