package com.rocketfuel.sdbc.base

trait ParameterizedQuery {
  self: ParameterValue =>

  /**
    * Given a query with named parameters beginning with '@',
    * construct the query for use with JDBC, so that names
    * are replaced by '?', and each parameter
    * has a map to its positions in the query.
    *
    * Parameter names must start with a unicode letter or underscore, and then
    * any character after the first one can be a unicode letter, unicode number,
    * or underscore. A parameter that does not follow
    * this scheme must be quoted by backticks. Parameter names
    * are case sensitive.
    *
    * Examples of identifiers:
    *
    * {{{"@hello"}}}
    *
    * {{{"@`hello there`"}}}
    *
    * {{{"@_i_am_busy"}}}
    */
  trait ParameterizedQuery[Self <: ParameterizedQuery[Self]] extends Logging {
    def statement: CompiledStatement

    def parameterValues: Map[String, ParameterValue]

    /**
      * The query text with name parameters replaced with positional parameters.
      *
      * @return
      */
    def queryText: String = statement.queryText

    def originalQueryText: String = statement.originalQueryText

    def parameterPositions: Map[String, Set[Int]] = statement.parameterPositions

    def unassignedParameters: Set[String] = parameterPositions.keySet -- parameterValues.keySet

    def clear: Self = subclassConstructor(parameterValues = Map.empty)

    def assign(parameters: Parameters): Self = {
      subclassConstructor(parameterValues = setParameters(parameters))
    }

    def assign(parameters: (String, ParameterValue)*): Self = {
      val asParameters: Parameters = parameters
      assign(asParameters)
    }

    protected def setParameters(parameters: Parameters): Map[String, ParameterValue] = {
      val parametersHavingPositions =
        parameters.parameters.filter(kvp => statement.parameterPositions.contains(kvp._1))
      parameterValues ++ parametersHavingPositions
    }

    protected def subclassConstructor(parameterValues: Map[String, ParameterValue]): Self

    def prepareStatement()(implicit connection: Connection): PreparedStatement

    protected def logExecution(parameters: Map[String, ParameterValue]): Unit = {
      logger.debug(s"""Executing "$originalQueryText" with parameters $parameters.""")
    }

  }

}
