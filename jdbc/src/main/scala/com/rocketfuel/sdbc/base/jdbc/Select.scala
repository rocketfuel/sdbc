package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logging

trait Select {
  self: DBMS =>

  case class Select[A] private[jdbc](
    override val statement: CompiledStatement,
    override val parameters: Parameters
  )(implicit rowConverter: RowConverter[A]
  ) extends ParameterizedQuery[Select[A]]
    with Executes {

    override def subclassConstructor(parameters: Parameters): Select[A] = {
      copy(parameters = parameters)
    }

    def iterator()(implicit connection: Connection): CloseableIterator[A] = {
      Select.iterator(statement, parameters)
    }

    def option()(implicit connection: Connection): Option[A] = {
      Select.option(statement, parameters)
    }

    override def execute()(implicit connection: Connection): Unit = {
      Execute.execute(statement, parameters)
    }

  }

  object Select
    extends Logging {

    def apply[A](
      queryText: String
    )(implicit rowConverter: RowConverter[A]
    ): Select[A] = {
      Select[A](
        statement = CompiledStatement(queryText),
        parameters = Map.empty[String, ParameterValue]
      )
    }

    /**
      * Construct the query without finding named parameters. No escaping will
      * need to be performed for a literal '@' to appear in the query. You will
      * not be able to use parameters when running this query.
      *
      * @param queryText
      * @param rowConverter
      * @tparam A
      * @return
      */
    def literal[A](
      queryText: String
    )(implicit rowConverter: RowConverter[A]
    ): Select[A] = {
      Select[A](
        statement = CompiledStatement.literal(queryText),
        parameters = Map.empty[String, ParameterValue]
      )
    }

    def iterator[A](
      queryText: String,
      parameterValues: Map[String, ParameterValue] = Map.empty
    )(implicit connection: Connection,
      rowConverter: RowConverter[A]
    ): CloseableIterator[A] = {
      val statement = CompiledStatement(queryText)

      iterator(statement, parameterValues)
    }

    def iterator[A](
      statement: CompiledStatement,
      parameterValues: Map[String, ParameterValue]
    )(implicit connection: Connection,
      rowConverter: RowConverter[A]
    ): CloseableIterator[A] = {
      logRun(statement, parameterValues)
      val executed = QueryMethods.execute(statement, parameterValues)
      StatementConverter.convertedRowIterator[A].apply(executed)
    }

    def option[A](
      queryText: String,
      parameterValues: Map[String, ParameterValue] = Map.empty
    )(implicit connection: Connection,
      rowConverter: RowConverter[A]
    ): Option[A] = {
      val statement = CompiledStatement(queryText)
      option(statement, parameterValues)
    }

    def option[A](
      statement: CompiledStatement,
      parameterValues: Map[String, ParameterValue]
    )(implicit connection: Connection,
      rowConverter: RowConverter[A]
    ): Option[A] = {
      logRun(statement, parameterValues)
      val executed = QueryMethods.execute(statement, parameterValues)
      StatementConverter.convertedRowOption.apply(executed)
    }

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Map[String, ParameterValue]
    ): Unit = {
      logger.debug(s"""Selecting "${compiledStatement.originalQueryText}" with parameters $parameters.""")
    }

  }

}
