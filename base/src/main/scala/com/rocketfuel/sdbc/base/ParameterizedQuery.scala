package com.rocketfuel.sdbc.base

import scala.collection._
import scala.collection.generic.Subtractable
import shapeless._

trait ParameterizedQuery {
  self: ParameterValue =>

  trait ParameterizedQuery[Self <: ParameterizedQuery[Self]]
    extends Subtractable[String, Self]
    with Logger {

    def statement: CompiledStatement

    def parameters: Parameters

    /**
      * The query text with name parameters replaced with positional parameters.
      *
      * @return
      */
    def queryText: String = statement.queryText

    def originalQueryText: String = statement.originalQueryText

    def parameterPositions: ParameterPositions = statement.parameterPositions

    /**
      * Parameters that you must set before running the query.
      */
    lazy val unassignedParameters: Set[String] = parameterPositions.keySet -- parameters.keySet

    /**
      * All the parameters have values.
      */
    def isComplete: Boolean =
      parameterPositions.size == parameters.size

    def +(parameterKvp: (String, ParameterValue)): Self =
      on(parameterKvp)

    def ++(parameterKvps: GenTraversableOnce[(String, ParameterValue)]): Self =
      onParameters(parameterKvps.toMap)

    /**
      * The same query, with no parameters having values.
      */
    def clear: Self = subclassConstructor(parameters = Parameters.empty)

    def on(additionalParameter: (String, ParameterValue), additionalParameters: (String, ParameterValue)*): Self = {
      onParameters((additionalParameter +: additionalParameters).toMap)
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
    )(implicit p: Parameters.Products[A, Repr, Key, AsParameters]
    ): Self = {
      subclassConstructor(Parameters.product(t))
    }

    def onRecord[
      Repr <: HList,
      Key <: Symbol,
      AsParameters <: HList
    ](t: Repr
    )(implicit r: Parameters.Records[Repr, Key, AsParameters]
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


    //Subtractable implementation
    override def -(parameterName: String): Self =
      subclassConstructor(parameters = parameters - parameterName)

    override protected def repr: Self = asInstanceOf[Self]
  }

}
