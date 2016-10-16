package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logging

trait Update {
  self: DBMS with Connection =>

  case class Update private (
    override val statement: CompiledStatement,
    override val parameters: Parameters
  ) extends ParameterizedQuery[Update]
    with Executes {

    override def subclassConstructor(parameters: Parameters): Update = {
      copy(parameters = parameters)
    }

    def update()(implicit connection: Connection): Long = {
      Update.update(statement, parameters)
    }

    def execute()(implicit connection: Connection): Unit = {
      Execute.execute(statement, parameters)
    }

  }

  object Update
    extends Logging {

    def apply(
      queryText: String,
      parameters: Parameters = Parameters.empty
    ): Update = {
      Update(
        statement = CompiledStatement(queryText),
        parameters = parameters
      )
    }

    /**
      * Construct the query without finding named parameters. No escaping will
      * need to be performed for a literal '@' to appear in the query. You will
      * not be able to use parameters when running this query.
      *
      * @param queryText
      * @return
      */
    def literal(
      queryText: String,
      parameters: Parameters = Parameters.empty
    ): Update = {
      Update(
        statement = CompiledStatement.literal(queryText),
        parameters = parameters
      )
    }

    def update(
      queryText: String,
      parameters: Parameters
    )(implicit connection: Connection
    ): Long = {
      val statement = CompiledStatement(queryText)
      logRun(statement, parameters)
      update(statement, parameters)
    }

    private[sdbc] def update(
      compiledStatement: CompiledStatement,
      parameters: Parameters
    )(implicit connection: Connection
    ): Long = {
      val runStatement = QueryMethods.execute(compiledStatement, parameters)
      try StatementConverter.update(runStatement).get
      finally runStatement.close()
    }

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Parameters
    ): Unit = {
      logger.debug(s"""Updating "${compiledStatement.originalQueryText}" with parameters $parameters.""")
    }

  }

}
