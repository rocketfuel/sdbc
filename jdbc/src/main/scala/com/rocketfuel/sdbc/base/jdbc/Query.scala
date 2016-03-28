package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.{CompiledStatement, Logging}
import shapeless.ops.hlist.ToList
import shapeless.ops.record.{Keys, MapValues}
import shapeless.syntax.std.tuple._
import shapeless.ops.tuple.{IsComposite, Mapper}
import shapeless._
import shapeless.ops.hlist
import shapeless.syntax.HListOps

trait Query {
  self: DBMS =>

  Query[(Unit, Unit, Iterator[ImmutableRow]), Result[(Result.Unit, Result.Unit, Result.ImmutableIterator)]]

  object Run extends Logging {

    private[jdbc] def bind(
      preparedStatement: PreparedStatement,
      compiledStatement: CompiledStatement,
      parameterValues: Map[String, ParameterValue]
    ): PreparedStatement = {
      for ((parameterName, parameterIndices) <- compiledStatement.parameterPositions) {
        val parameterValue = parameterValues(parameterName)
        for (parameterIndex <- parameterIndices) {
          parameterValue.set(preparedStatement, parameterIndex)
        }
      }

      preparedStatement
    }

    private[jdbc] def run(
      compiledStatement: CompiledStatement,
      parameterValues: Map[String, ParameterValue]
    )(implicit connection: Connection
    ): Statement = {
      logRun(compiledStatement, parameterValues)

      val prepared = connection.prepareStatement(compiledStatement.queryText)
      val bound = bind(prepared, compiledStatement, parameterValues)

      bound.execute()
      bound
    }

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Map[String, ParameterValue]
    ): Unit = {
      logger.debug(s"""Executing "${compiledStatement.originalQueryText}" with parameters $parameters.""")
    }

    def unapply[InnerResult, OuterResult <: Result[InnerResult]](
      query: Query[InnerResult, OuterResult]
    )(implicit connection: Connection
    ): Option[(java.lang.AutoCloseable, InnerResult)] = {
      val executedStatement = run(query.statement, query.parameterValues)
      val results = query.statementConverter(executedStatement)

      val closeable = new AutoCloseable {
        override def close(): Unit = executedStatement.close()
      }

      Some((closeable, results))
    }

  }

  class Query[InnerResult, OuterResult <: Result[InnerResult]] private[jdbc](
    override val statement: CompiledStatement,
    override val parameterValues: Map[String, ParameterValue]
  )(implicit val statementConverter: StatementConverter[InnerResult, OuterResult]
  ) extends ParameterizedQuery[Query[InnerResult, OuterResult]] {

    override def subclassConstructor(parameterValues: Map[String, ParameterValue]): Query[InnerResult, OuterResult] = {
      new Query[InnerResult, OuterResult](statement, parameterValues)
    }

    /**
      * Make a copy of this query that handles the results differently.
      *
      * @param statementConverter
      * @tparam B
      * @return
      */
    def as[InnerResult, OuterResult <: Result[InnerResult]](implicit statementConverter: StatementConverter[InnerResult, OuterResult]): Query[InnerResult, OuterResult] = {
      new Query[InnerResult, OuterResult](statement, parameterValues)
    }

  }

  object Query {

    def apply[InnerResult, OuterResult <: Result[InnerResult]](
      queryText: String,
      hasParameters: Boolean = true
    )(implicit statementConverter: StatementConverter[InnerResult, OuterResult]
    ): Query[InnerResult, OuterResult] = {
      new Query[InnerResult, OuterResult](
        statement = CompiledStatement(queryText, hasParameters),
        parameterValues = Map.empty[String, ParameterValue]
      )
    }

  }

}
