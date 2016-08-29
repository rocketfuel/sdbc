package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logging

trait Execute {
  self: DBMS =>

  trait Executes {
    /**
      * Runs the query, ignoring any results.
      * @param connection
      */
    def execute()(implicit connection: Connection): Unit
  }

  case class Execute(
    override val statement: CompiledStatement,
    override val parameterValues: Map[String, ParameterValue] = Map.empty
  ) extends ParameterizedQuery[Execute]
    with Executes {

    def execute()(implicit connection: Connection): Unit = {
      Execute.execute(statement, parameterValues)
    }

    override def subclassConstructor(parameterValues: Map[String, ParameterValue]): Execute = {
      copy(parameterValues = parameterValues)
    }
  }

  object Execute
    extends Logging {

    def apply(
      queryText: String
    ): Execute = {
      Execute(
        statement = CompiledStatement(queryText)
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
    ): Execute = {
      Execute(
        statement = CompiledStatement.literal(queryText)
      )
    }

    def execute(
      queryText: String,
      parameterValues: Map[String, ParameterValue]
    )(implicit connection: Connection
    ): Unit = {
      val statement = CompiledStatement(queryText)
      execute(statement, parameterValues)
    }

    def execute(
      statement: CompiledStatement,
      parameterValues: Map[String, ParameterValue]
    )(implicit connection: Connection
    ): Unit = {
      logRun(statement, parameterValues)
      val executed = QueryMethods.execute(statement, parameterValues)
      executed.close()
    }

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Map[String, ParameterValue]
    ): Unit = {
      logger.debug(s"""Executing "${compiledStatement.originalQueryText}" with parameters $parameters.""")
    }

  }

}
