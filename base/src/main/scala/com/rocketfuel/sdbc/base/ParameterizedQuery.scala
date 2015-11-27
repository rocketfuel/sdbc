package com.rocketfuel.sdbc.base

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
trait ParameterizedQuery[
  Self <: ParameterizedQuery[Self, UnderlyingQuery, Index],
  UnderlyingQuery,
  Index
] {

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

  /*
    * The goal here is to use generic programming to map over an generic type
    * to get maps from elems to parametervalues. The keys of the generic will provide
    * the names.
    */
  def on[A <: Product](t: A): Self = {
   val tAsParameters: CompositeParameter = t
    val newValues = setParameters(tAsparameters.parameters: _*)
    subclassConstructor(statement, newValues)
  }

  protected def setParameters(nameValuePairs: (String, Option[ParameterValue])*): Map[String, Option[Any]] = {
    nameValuePairs.foldLeft(parameterValues)(setParameter)
  }

}
