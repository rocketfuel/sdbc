package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.{Logging, CompiledStatement}

trait Update {
  self: DBMS =>

  case class Update private[jdbc](
    statement: CompiledStatement,
    parameterValues: Map[String, ParameterValue]
  ) extends base.Update[Connection]
  with ParameterizedQuery[Update]
  with Logging {

    override def update()(implicit connection: Connection): Long = {
      logger.debug( s"""Updating "$originalQueryText" with parameters $parameterValues.""")
      val prepared = prepare(
        queryText = queryText,
        parameterValues = parameterValues,
        parameterPositions = parameterPositions
      )

      val result = prepared.executeUpdate()
      prepared.close()
      result
    }

    override protected def subclassConstructor(
      statement: CompiledStatement,
      parameterValues: Map[String, ParameterValue]
    ): Update = {
      Update(statement, parameterValues)
    }
  }

  object Update {
    def apply(
      queryText: String,
      hasParameters: Boolean = true
    ): Update = {
      Update(
        statement = CompiledStatement(queryText, hasParameters),
        parameterValues = Map.empty[String, ParameterValue]
      )
    }
  }

}
