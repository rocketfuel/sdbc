package com.rocketfuel.sdbc.base

import shapeless._
import shapeless.ops.hlist._
import shapeless.ops.record.Keys

trait ParameterizedQuery {
  self: ParameterValue
    with CompositeSetter =>

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
      nameValuePair: (String, Option[ParameterValue])
    ): Map[String, Option[Any]] = {
      if (parameterPositions.contains(nameValuePair._1)) {
        val stripped = nameValuePair._2.map(p => p.value)
        parameterValues + (nameValuePair._1 -> stripped)
      } else {
        throw new IllegalArgumentException(s"${nameValuePair._1} is not a parameter in the query.")
      }
    }

    protected def subclassConstructor(
      statement: CompiledStatement,
      parameterValues: Map[String, Option[Any]]
    ): Self

    def on(parameterValues: (String, Option[ParameterValue])*): Self = {
      val newValues = setParameters(parameterValues: _*)
      subclassConstructor(statement, newValues)
    }

    protected def productParameters[
      A,
      Repr <: HList,
      MappedRepr <: HList,
      ReprKeys <: HList
    ](t: A
    )(implicit genericA: LabelledGeneric.Aux[A, Repr],
      mapper: Mapper.Aux[CompositeSetter.ToParameterValue.type, Repr, MappedRepr],
      keys: Keys.Aux[Repr, ReprKeys],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, Option[ParameterValue]]
    ): Seq[(String, Option[ParameterValue])] = {
      val setter = CompositeSetter.fromGeneric[A, Repr, MappedRepr, ReprKeys]
      setter(t)
    }

    def onProduct[
      A,
      Repr <: HList,
      MappedRepr <: HList,
      ReprKeys <: HList
    ](t: A
    )(implicit genericA: LabelledGeneric.Aux[A, Repr],
      mapper: Mapper.Aux[CompositeSetter.ToParameterValue.type, Repr, MappedRepr],
      keys: Keys.Aux[Repr, ReprKeys],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, Option[ParameterValue]]
    ): Self = {
      val newValues = setParameters(productParameters(t): _*)
      subclassConstructor(statement, newValues)
    }

    protected def recordParameters[
      Repr <: HList,
      MappedRepr <: HList,
      ReprKeys <: HList
    ](t: Repr
    )(implicit mapper: Mapper.Aux[CompositeSetter.ToParameterValue.type, Repr, MappedRepr],
      keys: Keys.Aux[Repr, ReprKeys],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, Option[ParameterValue]]
    ): Seq[(String, Option[ParameterValue])] = {
      val setter = CompositeSetter.fromRecord[Repr, MappedRepr, ReprKeys]
      setter(t)
    }

    def onRecord[
      Repr <: HList,
      MappedRepr <: HList,
      ReprKeys <: HList
    ](t: Repr
    )(implicit mapper: Mapper.Aux[CompositeSetter.ToParameterValue.type, Repr, MappedRepr],
      keys: Keys.Aux[Repr, ReprKeys],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, Option[ParameterValue]]
    ): Self = {
      val newValues = setParameters(recordParameters(t): _*)
      subclassConstructor(statement, newValues)
    }

    protected def setParameters(nameValuePairs: (String, Option[ParameterValue])*): Map[String, Option[Any]] = {
      nameValuePairs.foldLeft(parameterValues)(setParameter)
    }

  }

}
