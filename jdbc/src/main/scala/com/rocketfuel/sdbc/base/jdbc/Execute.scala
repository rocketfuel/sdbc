package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.{Logging, CompiledStatement}

trait Execute {
  self: ParameterValue
    with base.CompositeSetter
    with base.ParameterizedQuery =>

  case class Execute private[jdbc](
    statement: CompiledStatement,
    parameterValues: Map[String, Option[Any]]
  ) extends base.Execute[Connection]
  with ParameterizedQuery[Execute]
  with Logging {

    override def execute()(implicit connection: Connection): Unit = {
      logger.debug( s"""Executing "$originalQueryText" with parameters $parameterValues.""")
      val prepared = prepare(
        queryText = queryText,
        parameterValues = parameterValues,
        parameterPositions = parameterPositions
      )

      prepared.execute()
      prepared.close()
    }

    override protected def subclassConstructor(
      statement: CompiledStatement,
      parameterValues: Map[String, Option[Any]]
    ): Execute = {
      Execute(statement, parameterValues)
    }
  }

  object Execute {
    def apply(
      queryText: String,
      hasParameters: Boolean = true
    ): Execute = {
      Execute(
        statement = CompiledStatement(queryText, hasParameters),
        parameterValues = Map.empty[String, Option[Any]]
      )
    }
  }

}
