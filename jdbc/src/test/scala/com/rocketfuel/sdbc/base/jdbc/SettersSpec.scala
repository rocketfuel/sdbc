package com.rocketfuel.sdbc.base.jdbc

import java.sql.PreparedStatement

import com.rocketfuel.sdbc.base._
import org.scalatest.FunSuite
import shapeless._
import shapeless.syntax.singleton._

class SettersSpec
  extends FunSuite
  with Update
  with ParameterValue
  with DefaultSetters {

  implicit val parameterSetter: ParameterSetter = new ParameterSetter {
    def setAny(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Any): Unit = ???
  }

  val q = Update("@hi @bye")

  test("set a pair") {
    assertCompiles("""q.on("hi" -> 3)""")
  }

  test("set a record") {
    val params = ('hi ->> 3) :: ('bye ->> 4) :: HNil

    assertCompiles("""q.onRecord(params)""")
  }

  test("set a product") {
    case class Args(hi: Int, bye: Int)

    val param = Args(3, 4)

    assertCompiles("""q.onProduct(param)""")
  }

}
