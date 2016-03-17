package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.{Logging, CompiledStatement}
import shapeless.ops.hlist._
import shapeless.ops.record.{MapValues, Keys}
import shapeless.{LabelledGeneric, HList}

trait Query {
  self: DBMS =>

  case class Query[A] private[jdbc](
    override val statement: CompiledStatement,
    override val parameterValues: Map[String, ParameterValue]
  )(implicit statementConverter: StatementConverter[A]
  ) extends ParameterizedQuery[Query[A]] {

    override def subclassConstructor(parameterValues: Map[String, ParameterValue]): Query[A] = {
      copy(parameterValues = parameterValues)
    }

    protected def run(additionalParameters: Parameters)(implicit connection: Connection): A = {
      val withAdditionalParameters = setParameters(additionalParameters.parameters)
      Query.run(statement, withAdditionalParameters)
    }

    def run(additionalParameters: (String, ParameterValue)*)(implicit connection: Connection): A = {
      run(additionalParameters: Parameters)
    }

    def run(additionalParameters: Map[String, ParameterValue])(implicit connection: Connection): A = {
      run(additionalParameters: Parameters)
    }

    def run[
      P,
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](additionalParameters: P
    )(implicit connection: Connection,
      genericA: LabelledGeneric.Aux[P, Repr],
      keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): A = {
      run(additionalParameters: Parameters)
    }

    def run[
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](additionalParameters: Repr
    )(implicit connection: Connection,
      keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): A = {
      run(additionalParameters: Parameters)
    }

  }

  object Query
    extends Logging {

    def apply[A](
      queryText: String,
      hasParameters: Boolean = true
    )(implicit statementConverter: StatementConverter[A]
    ): Query[A] = {
      Query(
        statement = CompiledStatement(queryText, hasParameters),
        parameterValues = Map.empty[String, ParameterValue]
      )
    }

    def run[A](
      queryText: String,
      parameters: (String, ParameterValue)*
    )(implicit connection: Connection,
      statementConverter: StatementConverter[A]
    ): A = {
      val statement = CompiledStatement(queryText)
      run(statement, parameters.toMap)
    }

    private[jdbc] def prepareStatement(
      compiledStatement: CompiledStatement
    )(implicit connection: Connection
    ): PreparedStatement = {
      connection.prepareStatement(compiledStatement.queryText)
    }

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

      val prepared = prepareStatement(compiledStatement)
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

  }

}
