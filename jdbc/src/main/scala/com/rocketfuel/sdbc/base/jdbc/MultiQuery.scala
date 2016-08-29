package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logging
import com.rocketfuel.sdbc.base.jdbc.statement.MultiStatementConverter
import shapeless.ops.hlist._
import shapeless.ops.record.{Keys, MapValues}
import shapeless.{HList, LabelledGeneric}

/**
  * Add support for queries with multiple result sets, for use with DBMSs
  * that can return more than one ResultSet per statement.
  *
  */
trait MultiQuery extends MultiStatementConverter {
  self: DBMS =>

  case class MultiQuery[A](
    override val statement: CompiledStatement,
    override val parameterValues: Map[String, ParameterValue]
  )(implicit statementConverter: MultiStatementConverter[A]
  ) extends ParameterizedQuery[MultiQuery[A]] {

    override def subclassConstructor(parameterValues: Map[String, ParameterValue]): MultiQuery[A] = {
      copy(parameterValues = parameterValues)
    }

    protected def run(additionalParameters: Parameters)(implicit connection: Connection): A = {
      val withAdditionalParameters = setParameters(additionalParameters.parameters)
      MultiQuery.run(statement, withAdditionalParameters)
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

  object MultiQuery
    extends Logging {

    def apply[A](
      queryText: String
    )(implicit statementConverter: MultiStatementConverter[A]
    ): MultiQuery[A] = {
      MultiQuery[A](
        statement = CompiledStatement(queryText),
        parameterValues = Map.empty[String, ParameterValue]
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
        parameterValues = Map.empty[String, ParameterValue]
      )
    }

    def run[A](
      compiledStatement: CompiledStatement,
      parameterValues: Map[String, ParameterValue]
    )(implicit connection: Connection,
      statementConverter: MultiStatementConverter[A]
    ): A = {
      logRun(compiledStatement, parameterValues)

      val bound = QueryMethods.execute(compiledStatement, parameterValues)

      bound.execute()
      bound
    }

    def run[A](
      queryText: String,
      parameters: (String, ParameterValue)*
    )(implicit connection: Connection,
      statementConverter: MultiStatementConverter[A]
    ): A = {
      val statement = CompiledStatement(queryText)
      run(statement, parameters.toMap)
    }

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Map[String, ParameterValue]
    ): Unit = {
      logger.debug(s"""Executing "${compiledStatement.originalQueryText}" with parameters $parameters.""")
    }

  }


}
