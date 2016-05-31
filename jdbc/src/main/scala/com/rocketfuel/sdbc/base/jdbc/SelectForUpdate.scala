package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logging

trait SelectForUpdate {
  self: DBMS =>

  case class SelectForUpdate[A](
    override val statement: CompiledStatement,
    override val parameterValues: Map[String, ParameterValue] = Map.empty
  )(implicit rowConverter: RowConverter[UpdatableRow, A]
  ) extends ParameterizedQuery[SelectForUpdate[A]]
    with Executes {

    override def subclassConstructor(parameterValues: Map[String, ParameterValue]): SelectForUpdate[A] = {
      copy(parameterValues = parameterValues)
    }

    def iterator()(implicit connection: Connection): CloseableIterator[A] = {
      SelectForUpdate.iterator(statement, parameterValues)
    }

    def option()(implicit connection: Connection): Option[A] = {
      SelectForUpdate.option[A](statement, parameterValues)
    }

    def execute()(implicit connection: Connection): Unit = {
      Execute.execute(statement, parameterValues)
    }

  }

  object SelectForUpdate
    extends Logging {

    def apply[A](
      queryText: String
    )(implicit rowConverter: RowConverter[UpdatableRow, A]
    ): SelectForUpdate[A] = {
      SelectForUpdate[A](
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
    )(implicit rowConverter: RowConverter[UpdatableRow, A]
    ): SelectForUpdate[A] = {
      SelectForUpdate[A](
        statement = CompiledStatement.literal(queryText),
        parameterValues = Map.empty[String, ParameterValue]
      )
    }

    def iterator[A](
      queryText: String,
      parameterValues: Map[String, ParameterValue]
    )(implicit connection: Connection,
      rowConverter: RowConverter[Row, A]
    ): CloseableIterator[A] = {
      val statement = CompiledStatement(queryText)
      logRun(statement, parameterValues)
      iterator(statement, parameterValues)
    }

    def iterator[A](
      statement: CompiledStatement,
      parameterValues: Map[String, ParameterValue]
    )(implicit connection: Connection,
      rowConverter: RowConverter[UpdatableRow, A]
    ): CloseableIterator[A] = {
      val executed = QueryMethods.executeForUpdate(statement, parameterValues)
      StatementConverter.convertedRowIterator[UpdatableRow, A].apply(executed)
    }

    def option[A](
      queryText: String,
      parameterValues: Map[String, ParameterValue]
    )(implicit connection: Connection,
      rowConverter: RowConverter[UpdatableRow, A]
    ): Option[A] = {
      val statement = CompiledStatement(queryText)
      logRun(statement, parameterValues)
      option(statement, parameterValues)
    }

    def option[A](
      statement: CompiledStatement,
      parameterValues: Map[String, ParameterValue]
    )(implicit connection: Connection,
      rowConverter: RowConverter[UpdatableRow, A]
    ): Option[A] = {
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
      logger.debug(s"""Selecting for update "${compiledStatement.originalQueryText}" with parameters $parameters.""")
    }

  }

}
