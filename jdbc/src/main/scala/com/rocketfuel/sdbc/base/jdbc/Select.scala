package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logging

trait Select {
  self: DBMS =>

  case class Select[A] private[jdbc](
    override val statement: CompiledStatement,
    override val parameterValues: Map[String, ParameterValue]
  )(implicit rowConverter: RowConverter[Row, A]
  ) extends ParameterizedQuery[Select[A]]
    with Executes {

    override def subclassConstructor(parameterValues: Map[String, ParameterValue]): Select[A] = {
      copy(parameterValues = parameterValues)
    }

    def iterator()(implicit connection: Connection): CloseableIterator[A] = {
      Select.iterator(statement, parameterValues)
    }

    def option()(implicit connection: Connection): Option[A] = {
      Select.option(statement, parameterValues)
    }

    def execute()(implicit connection: Connection): Unit = {
      Execute.execute(statement, parameterValues)
    }

  }

  object Select
    extends Logging {

    def apply[A](
      queryText: String
    )(implicit rowConverter: RowConverter[Row, A]
    ): Select[A] = {
      Select[A](
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
      * @param rowConverter
      * @tparam A
      * @return
      */
    def literal[A](
      queryText: String
    )(implicit rowConverter: RowConverter[Row, A]
    ): Select[A] = {
      Select[A](
        statement = CompiledStatement.literal(queryText),
        parameterValues = Map.empty[String, ParameterValue]
      )
    }

    def iterator[A](
      queryText: String,
      parameterValues: Map[String, ParameterValue] = Map.empty
    )(implicit connection: Connection,
      rowConverter: RowConverter[Row, A]
    ): CloseableIterator[A] = {
      val statement = CompiledStatement(queryText)

      logRun(statement, parameterValues)
      val executed = QueryMethods.execute(statement, parameterValues)
      StatementConverter.convertedRowIterator[Row, A].apply(executed)
    }

    def iterator[A](
      statement: CompiledStatement,
      parameterValues: Map[String, ParameterValue]
    )(implicit connection: Connection,
      rowConverter: RowConverter[Row, A]
    ): CloseableIterator[A] = {
      val executed = QueryMethods.execute(statement, parameterValues)
      StatementConverter.convertedRowIterator[Row, A].apply(executed)
    }

    def option[A](
      queryText: String,
      parameterValues: Map[String, ParameterValue] = Map.empty
    )(implicit connection: Connection,
      rowConverter: RowConverter[Row, A]
    ): Option[A] = {
      val statement = CompiledStatement(queryText)
      option(statement, parameterValues)
    }

    def option[A](
      statement: CompiledStatement,
      parameterValues: Map[String, ParameterValue]
    )(implicit connection: Connection,
      rowConverter: RowConverter[Row, A]
    ): Option[A] = {
      logRun(statement, parameterValues)
      val iterator = this.iterator(statement, parameterValues)
      try {
        if (iterator.hasNext) Some(iterator.next())
        else None
      } finally iterator.close()
    }

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Map[String, ParameterValue]
    ): Unit = {
      logger.debug(s"""Selecting "${compiledStatement.originalQueryText}" with parameters $parameters.""")
    }

  }

}
