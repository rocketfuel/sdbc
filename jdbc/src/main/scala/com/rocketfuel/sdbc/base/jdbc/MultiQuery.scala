package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logging
import com.rocketfuel.sdbc.base.jdbc.statement.MultiStatementConverter

/**
  * Add support for queries with multiple result sets, for use with DBMSs
  * that can return more than one ResultSet per statement.
  *
  */
trait MultiQuery extends MultiStatementConverter {
  self: DBMS =>

  case class MultiQuery[A](
    override val statement: CompiledStatement,
    override val parameters: Parameters
  )(implicit statementConverter: MultiStatementConverter[A]
  ) extends ParameterizedQuery[MultiQuery[A]] {

    override def subclassConstructor(parameters: Parameters): MultiQuery[A] = {
      copy(parameters = parameters)
    }

    def run()(implicit connection: Connection): A = {
      MultiQuery.run(statement, parameters)
    }

  }

  object MultiQuery
    extends Logging {

    def apply[A](
      queryText: String
    )(implicit statementConverter: MultiStatementConverter[A]
    ): MultiQuery[A] = {
      MultiQuery[A](
        statement = CompiledStatement(queryText),
        parameters = Map.empty[String, ParameterValue]
      )
    }

    /**
      * Construct the query without named parameters. No escaping will
      * need to be performed for a literal '@' to appear in the query.
      *
      * @param queryText
      * @param statementConverter
      * @tparam A
      * @return
      */
    def literal[A](
      queryText: String
    )(implicit statementConverter: MultiStatementConverter[A]
    ): MultiQuery[A] = {
      MultiQuery[A](
        statement = CompiledStatement.literal(queryText),
        parameters = Map.empty[String, ParameterValue]
      )
    }

    def run[A](
      compiledStatement: CompiledStatement,
      parameters: Parameters
    )(implicit connection: Connection,
      statementConverter: MultiStatementConverter[A]
    ): A = {
      logRun(compiledStatement, parameters)

      val bound = QueryMethods.execute(compiledStatement, parameters)

      bound.execute()
      bound
    }

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Parameters
    ): Unit = {
      logger.debug(s"""Executing "${compiledStatement.originalQueryText}" with parameters $parameters.""")
    }

  }


}
