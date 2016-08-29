package com.rocketfuel.sdbc.base

import shapeless.ops.hlist._
import shapeless.ops.record.{Keys, MapValues}
import shapeless.{HList, LabelledGeneric}

trait ParameterizedQuery {
  self: ParameterValue =>

  trait ParameterizedQuery[Self <: ParameterizedQuery[Self]] extends Logging {

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

    def onParameters(additionalParameters: Map[String, ParameterValue]): Self = {
      val withAdditionalParameters = setParameters(additionalParameters)
      subclassConstructor(parameterValues = withAdditionalParameters)
    }

    def on(additionalParameter: (String, ParameterValue), additionalParameters: (String, ParameterValue)*): Self = {
      onParameters((additionalParameter +: additionalParameters: Parameters).parameters)
    }

    def onProduct[
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
      onParameters((additionalParameters: Parameters).parameters)
    }

    def onRecord[
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](additionalParameters: Repr
    )(implicit keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Self = {
      onParameters((additionalParameters: Parameters).parameters)
    }

    protected def setParameters(parameters: Map[String, ParameterValue]): Map[String, ParameterValue] = {
      val parametersHavingPositions =
        parameters.filter(kvp => statement.parameterPositions.contains(kvp._1))
      parameterValues ++ parametersHavingPositions
    }

    protected def subclassConstructor(parameterValues: Map[String, ParameterValue]): Self

    def logExecution(): Unit =
      logger.debug(s"""Executing "$originalQueryText" with parameters $parameterValues.""")
  }

}
