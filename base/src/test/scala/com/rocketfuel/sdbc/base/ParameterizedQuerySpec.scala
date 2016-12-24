package com.rocketfuel.sdbc.base

import org.scalatest.FunSuite

class ParameterizedQuerySpec
  extends FunSuite
  with ParameterValue
  with ParameterizedQuery {

  override type PreparedStatement = Nothing

  override protected def setNone(
    preparedStatement: PreparedStatement,
    parameterIndex: Int
  ): PreparedStatement = ???

  case class Query(
    override val statement: CompiledStatement,
    override val parameters: Parameters = Parameters.empty
  ) extends ParameterizedQuery[Query] {
    override protected def subclassConstructor(parameters: Parameters): Query =
      copy(parameters = parameters)
  }

  test("Identifier that appears as a word in the text can be assigned a value.") {
    val query = Query("@t").on("t" -> None)

    assertResult(
      Map("t" -> ParameterValue.empty)
    )(query.parameters
    )
  }

  test("Identifier that does not appear as a word in the text is not assigned a value.") {
    val query = Query("@t").on("u" -> None)

    assertResult(
      Map.empty
    )(query.parameters
    )
  }

}
