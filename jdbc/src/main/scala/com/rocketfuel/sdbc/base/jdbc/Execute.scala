package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.{CompiledStatement, Logging}

trait Execute {
  self: DBMS =>

  trait Executes {
    /**
      * Runs the query, ignoring any results.
      * @param connection
      */
    def execute()(implicit connection: Connection): Unit
  }

  case class Execute private [jdbc] (
    override val statement: CompiledStatement,
    override val parameterValues: Map[String, ParameterValue]
  ) extends ParameterizedQuery[Execute]
    with Executes {

    def execute()(implicit connection: Connection): Unit = {
      Execute.logRun(statement, parameterValues)

      val executed = QueryMethods.execute(
        compiledStatement = statement,
        parameterValues = parameterValues
      )

      executed.close()
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
    ): Execute = {
      Execute(
        statement = CompiledStatement.literal(queryText),
        parameterValues = Map.empty[String, ParameterValue]
      )
    }

    def execute(
      queryText: String,
      parameterValues: Map[String, ParameterValue] = Map.empty[String, ParameterValue]
    )(implicit connection: Connection
    ): Unit = {
      val statement = CompiledStatement(queryText)
      logRun(statement, parameterValues)
      execute(statement, parameterValues)
    }

    private[jdbc] def execute(
      statement: CompiledStatement,
      parameterValues: Map[String, ParameterValue]
    )(implicit connection: Connection
    ): Unit = {
      val executed = QueryMethods.execute(statement, parameterValues)
      executed.close()
    }

    def executeLiteral(
      queryText: String
    )(implicit connection: Connection
    ): Unit = {
      val statement = CompiledStatement.literal(queryText)

      logRun(statement, Map.empty)
      val executed = QueryMethods.execute(statement, Map.empty)
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
