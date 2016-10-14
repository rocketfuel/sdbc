package com.rocketfuel.sdbc.base

import shapeless.{HList, LabelledGeneric}
import shapeless.ops.hlist.{Mapper, _}
import shapeless.ops.record.{Keys, Values}

trait ParameterizedQuery {
  self: ParameterValue =>

  trait ParameterizedQuery[Self <: ParameterizedQuery[Self]] extends Logging {

    def statement: CompiledStatement

    def parameters: Parameters

    /**
      * The query text with name parameters replaced with positional parameters.
      *
      * @return
      */
    def queryText: String = statement.queryText

    def originalQueryText: String = statement.originalQueryText

    def parameterPositions: Map[String, Set[Int]] = statement.parameterPositions

    def unassignedParameters: Set[String] = parameterPositions.keySet -- parameters.keySet

    def clear: Self = subclassConstructor(parameters = Parameters.empty)

    def on(additionalParameter: (String, ParameterValue), additionalParameters: (String, ParameterValue)*): Self = {
      onParameters(Map((additionalParameter +: additionalParameters): _*))
    }

    def onParameters(additionalParameters: Parameters): Self = {
      subclassConstructor(parameters ++ additionalParameters)
    }

    def onProduct[
      A,
      Repr <: HList,
      ReprKeys <: HList,
      ReprValues <: HList,
      MappedRepr <: HList
    ](t: A
    )(implicit genericA: LabelledGeneric.Aux[A, Repr],
      keys: Keys.Aux[Repr, ReprKeys],
      values: Values.Aux[Repr, ReprValues],
      valuesMapper: Mapper.Aux[ToParameterValue.type, ReprValues, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Self = {
      subclassConstructor(Parameters.product(t))
    }

    def onRecord[
      Repr <: HList,
      ReprKeys <: HList,
      ReprValues <: HList,
      MappedRepr <: HList
    ](t: Repr
    )(implicit keys: Keys.Aux[Repr, ReprKeys],
      values: Values.Aux[Repr, ReprValues],
      valuesMapper: Mapper.Aux[ToParameterValue.type, ReprValues, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Self = {
      subclassConstructor(Parameters.record(t))
    }

    protected def filter(p: Parameters): Parameters = {
      p.filter(kvp => parameterPositions.contains(kvp._1))
    }

    protected def setParameters(parameters: Parameters): Parameters = {
      val parametersHavingPositions = filter(parameters)
      parameters ++ parametersHavingPositions
    }

    protected def subclassConstructor(parameters: Parameters): Self

    def logExecution(): Unit =
      logger.debug(s"""Executing "$originalQueryText" with parameters $parameters.""")
  }

}
