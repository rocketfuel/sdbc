package com.rocketfuel.sdbc.base.jdbc

import java.sql.ResultSet

private[jdbc] trait QueryMethods {
  self: DBMS with Connection =>

  private[jdbc] object QueryMethods {
    def bind(
      preparedStatement: PreparedStatement,
      compiledStatement: CompiledStatement,
      parameters: Parameters
    ): PreparedStatement = {
      for ((parameterName, parameterIndices) <- compiledStatement.parameterPositions) {
        val parameterValue = parameters(parameterName)
        for (parameterIndex <- parameterIndices) {
          parameterValue.set(preparedStatement, parameterIndex)
        }
      }

      preparedStatement
    }

    def execute(
      compiledStatement: CompiledStatement,
      parameters: Parameters,
      resultSetType: Int = ResultSet.TYPE_FORWARD_ONLY,
      resultSetConcurrency: Int = ResultSet.CONCUR_READ_ONLY
    )(implicit connection: Connection
    ): PreparedStatement = {
      val prepared = connection.prepareStatement(compiledStatement.queryText, resultSetType, resultSetConcurrency)
      try prepared.closeOnCompletion()
      catch {case _: AbstractMethodError => /* this is for jtds */}
      val bound = bind(prepared, compiledStatement, parameters)

      bound.execute()
      bound
    }

    def executeForUpdate(
      compiledStatement: CompiledStatement,
      parameters: Parameters
    )(implicit connection: Connection
    ): PreparedStatement = {
      execute(compiledStatement, parameters, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
    }
  }

}
