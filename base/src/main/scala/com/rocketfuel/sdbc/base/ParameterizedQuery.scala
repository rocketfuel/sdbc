package com.rocketfuel.sdbc.base

import shapeless.ops.hlist._
import shapeless.ops.record.{MapValues, Keys}
import shapeless.{LabelledGeneric, HList}

trait ParameterizedQuery {
  self: ParameterValue =>

  /**
    * Given a query with named parameters beginning with '@',
    * construct the query for use with JDBC, so that names
    * are replaced by '?', and each parameter
    * has a map to its positions in the query.
    *
    * Parameter names must start with a unicode letter or underscore, and then
    * any character after the first one can be a unicode letter, unicode number,
    * or underscore. A parameter that does not follow
    * this scheme must be quoted by backticks. Parameter names
    * are case sensitive.
    *
    * Examples of identifiers:
    *
    * {{{"@hello"}}}
    *
    * {{{"@`hello there`"}}}
    *
    * {{{"@_i_am_busy"}}}
    */
  trait ParameterizedQuery[Self <: ParameterizedQuery[Self]] {

    def statement: CompiledStatement

    def parameterValues: Map[String, ParameterValue]

    /**
      * The query text with name parameters replaced with positional parameters.
      *
      * @return
      */
    def queryText: String = statement.queryText

    def originalQueryText: String = statement.originalQueryText

    def parameterPositions: Map[String, Set[Int]] = statement.parameterPositions

    def unassignedParameters: Set[String] = parameterPositions.keySet -- parameterValues.keySet

    def clear: Self = subclassConstructor(parameterValues = Map.empty)

    protected def on(additionalParameters: Parameters): Self = {
      val withAdditionalParameters = setParameters(additionalParameters.parameters)
      subclassConstructor(parameterValues = withAdditionalParameters)
    }

    def on(additionalParameters: Map[String, ParameterValue]): Self = {
      on(additionalParameters: Parameters)
    }

    def on(additionalParameters: (String, ParameterValue)*): Self = {
      on(additionalParameters: Parameters)
    }

    def on[
      P,
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](additionalParameters: P
    )(implicit genericA: LabelledGeneric.Aux[P, Repr],
      keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Self = {
      on(additionalParameters: Parameters)
    }

    def on[
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](additionalParameters: Repr
    )(implicit keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Self = {
      on(additionalParameters: Parameters)
    }

    protected def setParameters(parameters: Map[String, ParameterValue]): Map[String, ParameterValue] = {
      val parametersHavingPositions =
        parameters.filter(kvp => statement.parameterPositions.contains(kvp._1))
      parameterValues ++ parametersHavingPositions
    }

    protected def subclassConstructor(parameterValues: Map[String, ParameterValue]): Self

  }

}
