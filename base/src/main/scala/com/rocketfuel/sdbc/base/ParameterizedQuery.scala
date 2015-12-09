package com.rocketfuel.sdbc.base

import shapeless._
import shapeless.record._
import shapeless.ops.record._
import shapeless.ops.hlist._

trait ParameterizedQuery {
  self: ParameterValue =>

  object ToParameterValue extends Poly {
    implicit def fromValue[A](implicit parameter: Parameter[A]) = {
      use {
        (value: A) =>
          ParameterValue[A](value)
      }
    }
  }

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

    def parameterValues: Map[String, Option[Any]]

    /**
      * The query text with name parameters replaced with positional parameters.
      * @return
      */
    def queryText: String = statement.queryText

    def originalQueryText: String = statement.originalQueryText

    def parameterPositions: Map[String, Set[Int]] = statement.parameterPositions

    private def setParameter(
      parameterValues: Map[String, Option[Any]],
      nameValuePair: (String, ParameterValue)
    ): Map[String, Option[Any]] = {
      if (parameterPositions.contains(nameValuePair._1)) {
        val stripped = nameValuePair._2.value
        parameterValues + (nameValuePair._1 -> stripped)
      } else {
        throw new IllegalArgumentException(s"${nameValuePair._1} is not a parameter in the query.")
      }
    }

    protected def subclassConstructor(
      statement: CompiledStatement,
      parameterValues: Map[String, Option[Any]]
    ): Self

    def on(parameterValues: (String, ParameterValue)*): Self = {
      val newValues = setParameters(parameterValues: _*)
      subclassConstructor(statement, newValues)
    }

    protected def productParameters[
      A,
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](t: A
    )(implicit genericA: LabelledGeneric.Aux[A, Repr],
      keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): ParameterList = {
      val asGeneric = genericA.to(t)
      recordParameters(asGeneric)
    }

    def onProduct[
      A,
      Repr <: HList,
      MappedRepr <: HList,
      ReprKeys <: HList
    ](t: A
    )(implicit genericA: LabelledGeneric.Aux[A, Repr],
      keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Self = {
      val newValues = setParameters(productParameters(t): _*)
      subclassConstructor(statement, newValues)
    }

    protected def recordParameters[
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](t: Repr
    )(implicit keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): ParameterList = {
      val mapped = t.mapValues(ToParameterValue)
      t.keys.toList.map(_.name) zip mapped.toList
    }

    def onRecord[
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](t: Repr
    )(implicit keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Self = {
      val newValues = setParameters(recordParameters(t): _*)
      subclassConstructor(statement, newValues)
    }

    protected def setParameters(nameValuePairs: (String, ParameterValue)*): Map[String, Option[Any]] = {
      nameValuePairs.foldLeft(parameterValues)(setParameter)
    }

  }

}
