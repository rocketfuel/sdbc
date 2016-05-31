package com.rocketfuel.sdbc.base.jdbc

import java.sql.ResultSet

private[jdbc] trait QueryMethods {
  self: DBMS =>

  private[jdbc] object QueryMethods {
    def bind(
      preparedStatement: PreparedStatement,
      compiledStatement: CompiledStatement,
      parameterValues: Map[String, ParameterValue]
    ): PreparedStatement = {
      for ((parameterName, parameterIndices) <- compiledStatement.parameterPositions) {
        val parameterValue = parameterValues(parameterName)
        for (parameterIndex <- parameterIndices) {
          parameterValue.set(preparedStatement, parameterIndex)
        }
      }

      preparedStatement
    }

    def execute(
      compiledStatement: CompiledStatement,
      parameterValues: Map[String, ParameterValue]
    )(implicit connection: Connection
    ): PreparedStatement = {
      val prepared = connection.prepareStatement(compiledStatement.queryText)
      val bound = bind(prepared, compiledStatement, parameterValues)

      bound.execute()
      bound
    }

    def executeForUpdate(
      compiledStatement: CompiledStatement,
      parameterValues: Map[String, ParameterValue]
    )(implicit connection: Connection
    ): PreparedStatement = {
      val prepared = connection.prepareStatement(compiledStatement.queryText, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
      val bound = bind(prepared, compiledStatement, parameterValues)

      bound.execute()
      bound
    }
  }

}
