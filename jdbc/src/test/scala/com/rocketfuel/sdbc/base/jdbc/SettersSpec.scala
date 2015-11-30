package com.rocketfuel.sdbc.base.jdbc

import java.sql.PreparedStatement

import com.rocketfuel.sdbc.base.ParameterValueImplicits
import org.scalatest.FunSuite
import shapeless._
import shapeless.syntax.singleton._

class SettersSpec
  extends FunSuite
  with DefaultSetters
  with ParameterValueImplicits {

  implicit val parameterSetter: ParameterSetter = new ParameterSetter {
    def setAny(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Any): Unit = ???
  }

  val q = Update("@hi @bye")

  test("") {

  }

  case class Woozle(a: (Int, Int), b: Int :: Int :: HNil, c: Int)

  test("set a pair") {
    q.on("hi" -> 3)
    assertCompiles("""q.on("hi" -> 3)""")
  }

  test("set a record") {
    val params = ('hi ->> 3) :: ('bye ->> 4) :: HNil

    q.onRecord(params)

    assertCompiles("""q.onRecord(params)""")
  }

  test("set a product") {
    case class Args(hi: Int, bye: Int)

    val param = Args(3, 4)

    assertCompiles("""q.onGeneric(param)""")
  }

//  test("implicit Unit conversion works") {
//    val u = Record.`'i -> 3`
//    val setter = CompositeSetter.fromRecord.apply(u)
//    val toCheck: CompositeParameter = CompositeParameter(setter(u))
//    assertResult(HNil)(toCheck)
//  }

  //  test("CompositeSetter[Woozle] exists") {
  //    assertCompiles("implicitly[CompositeSetter[Woozle]]")
  //  }
  //
  //  test("implicit Tuple2 conversion works") {
  //    assertCompiles("val _: CompositeParameter = CompositeParameter((3, 3))")
  //  }
  //
  //  test("implicit Record conversion works") {
  //    assertCompiles("val _: CompositeParameter = (\"hi\" ->> 3) :: HNil")
  //  }

}
