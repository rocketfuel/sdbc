package com.rocketfuel.sdbc.base

trait CompiledQuery {

  def statement: CompiledStatement

  /**
    * The query text with name parameters replaced with positional parameters.
    *
    * @return
    */
  def queryText: String = statement.queryText

  def originalQueryText: String = statement.originalQueryText

  def parameterPositions: Map[String, Set[Int]] = statement.parameterPositions

}

trait CompiledParameterizedQuery
  extends ParameterizedQuery {
  self: ParameterValue =>

  trait CompiledParameterizedQuery[Self <: CompiledParameterizedQuery[Self]]
    extends ParameterizedQuery[Self]
    with CompiledQuery

}
