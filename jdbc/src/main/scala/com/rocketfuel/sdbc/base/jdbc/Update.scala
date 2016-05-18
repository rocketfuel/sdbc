package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.{Logging, CompiledStatement}

trait Update {
  self: DBMS =>

  case class Update private[jdbc](
    override val statement: CompiledStatement,
    override val parameterValues: Map[String, ParameterValue]
  ) extends ParameterizedQuery[Update]
    with Executes {

    override def subclassConstructor(parameterValues: Map[String, ParameterValue]): Update = {
      copy(parameterValues = parameterValues)
    }

    def update()(implicit connection: Connection): Long = {
      Update.update(statement, parameterValues)
    }

    def execute()(implicit connection: Connection): Unit = {
      Execute.execute(statement, parameterValues)
    }

  }

  object Update
    extends Logging {

    def apply(
      queryText: String
    ): Update = {
      Update(
        statement = CompiledStatement(queryText),
        parameterValues = Map.empty[String, ParameterValue]
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
      queryText: String
    ): Update = {
      Update(
        statement = CompiledStatement.literal(queryText),
        parameterValues = Map.empty[String, ParameterValue]
      )
    }

    def update(
      queryText: String,
      parameterValues: Map[String, ParameterValue] = Map.empty
    )(implicit connection: Connection
    ): Long = {
      val statement = CompiledStatement(queryText)
      logRun(statement, parameterValues)
      update(statement, parameterValues)
    }

    private[sdbc] def update(
      compiledStatement: CompiledStatement,
      parameterValues: Map[String, ParameterValue]
    )(implicit connection: Connection
    ): Long = {
      val runStatement = QueryMethods.execute(compiledStatement, parameterValues)
      try StatementConverter.update(runStatement).get
      finally runStatement.close()
    }

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Map[String, ParameterValue]
    ): Unit = {
      logger.debug(s"""Updating "${compiledStatement.originalQueryText}" with parameters $parameters.""")
    }

  }

}
