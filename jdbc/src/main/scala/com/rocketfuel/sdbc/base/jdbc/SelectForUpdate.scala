package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logging

trait SelectForUpdate {
  self: DBMS with Connection =>

  case class SelectForUpdate(
    override val statement: CompiledStatement,
    override val parameters: Parameters = Parameters.empty
  ) extends IgnorableQuery[SelectForUpdate] {

    override def subclassConstructor(parameters: Parameters): SelectForUpdate = {
      copy(parameters = parameters)
    }

    def iterator()(implicit connection: Connection): CloseableIterator[UpdatableRow] = {
      SelectForUpdate.iterator(statement, parameters)
    }

  }

  object SelectForUpdate
    extends Logging {

    def iterator[A](
      statement: CompiledStatement,
      parameterValues: Parameters
    )(implicit connection: Connection
    ): CloseableIterator[UpdatableRow] = {
      val executed = QueryMethods.executeForUpdate(statement, parameterValues)
      StatementConverter.updatableResults(executed)
    }

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Parameters
    ): Unit = {
      logger.debug(s"""Selecting for update "${compiledStatement.originalQueryText}" with parameters $parameters.""")
    }

  }

}
