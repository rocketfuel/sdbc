package com.rocketfuel.sdbc.base

import shapeless._
import shapeless.ops.record._

trait ParameterizedQuery {
  self: ParameterValue =>

  trait ParameterizedQuery[Self <: ParameterizedQuery[Self]] extends Logger {

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

    lazy val unassignedParameters: Set[String] = parameterPositions.keySet -- parameters.keySet

    def clear: Self = subclassConstructor(parameters = Parameters.empty)

    def on(additionalParameter: (String, ParameterValue), additionalParameters: (String, ParameterValue)*): Self = {
      onParameters(Map((additionalParameter +: additionalParameters): _*))
    }

    def onParameters(additionalParameters: Parameters): Self = {
      subclassConstructor(setParameters(additionalParameters))
    }

    def onProduct[
      A,
      Repr <: HList,
      Key <: Symbol,
      AsParameters <: HList
    ](t: A
    )(implicit genericA: LabelledGeneric.Aux[A, Repr],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, AsParameters],
      toMap: ToMap.Aux[AsParameters, Key, ParameterValue]
    ): Self = {
      subclassConstructor(Parameters.product(t))
    }

    def onRecord[
      Repr <: HList,
      Key <: Symbol,
      AsParameters <: HList
    ](t: Repr
    )(implicit valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, AsParameters],
      toMap: ToMap.Aux[AsParameters, Key, ParameterValue]
    ): Self = {
      subclassConstructor(Parameters.record(t))
    }

    protected def filter(p: Parameters): Parameters = {
      p.filter(kvp => parameterPositions.contains(kvp._1))
    }

    protected def setParameters(additionalParameters: Parameters): Parameters = {
      val parametersHavingPositions = filter(additionalParameters)
      parameters ++ parametersHavingPositions
    }

    protected def subclassConstructor(parameters: Parameters): Self

  }

}
