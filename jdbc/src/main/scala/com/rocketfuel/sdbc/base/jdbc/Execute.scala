package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logging

trait Execute {
  self: DBMS with Connection =>

  trait Executes {
    /**
      * Runs the query, ignoring any results.
      * @param connection
      */
    def execute()(implicit connection: Connection): Unit
  }

  case class Execute private (
    override val statement: CompiledStatement,
    override val parameters: Parameters = Parameters.empty
  ) extends ParameterizedQuery[Execute]
    with Executes {

    def execute()(implicit connection: Connection): Unit = {
      Execute.execute(statement, parameters)
    }

    override def subclassConstructor(parameters: Parameters): Execute = {
      copy(parameters = parameters)
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
      parameters: Parameters
    )(implicit connection: Connection
    ): Unit = {
      val statement = CompiledStatement(queryText)
      execute(statement, parameters)
    }

    def execute(
      statement: CompiledStatement,
      parameters: Parameters
    )(implicit connection: Connection
    ): Unit = {
      logRun(statement, parameters)
      val executed = QueryMethods.execute(statement, parameters)
      executed.close()
    }

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Parameters
    ): Unit = {
      logger.debug(s"""Executing "${compiledStatement.originalQueryText}" with parameters $parameters.""")
    }

  }

}
