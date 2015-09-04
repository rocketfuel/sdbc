package com.rocketfuel.sdbc.cassandra.datastax

import com.rocketfuel.Logging
import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.CompiledStatement
import com.datastax.driver.core.{Row => CRow, _}
import scala.concurrent._
import scala.collection.convert.decorateAsScala._

case class Select[T] private [cassandra] (
  override val statement: CompiledStatement,
  override val parameterValues: Map[String, Option[Any]],
  override val queryOptions: QueryOptions
)(implicit val converter: CRow => T)
  extends base.Select[Session, T]
  with implementation.ParameterizedQuery[Select[T]]
  with implementation.HasQueryOptions
  with Logging {

  def iterator()(implicit session: Session): Iterator[T] = {
    logger.debug(s"""Retrieving an iterator using "$originalQueryText" with parameters $parameterValues.""")
    val prepared = implementation.prepare(statement, parameterValues, queryOptions)
    session.execute(prepared).iterator.asScala.map(converter)
  }

  def iteratorAsync()(implicit session: Session, ec: ExecutionContext): Future[Iterator[T]] = {
    logger.debug(s"""Asynchronously retrieving an iterator asynchronously using "$originalQueryText" with parameters $parameterValues.""")

    val prepared = implementation.prepare(statement, parameterValues, queryOptions)
    val toListen = session.executeAsync(prepared)

    for {
      result <- implementation.toScalaFuture(toListen)
    } yield {
      result.iterator().asScala.map(converter)
    }
  }

  override def subclassConstructor(
    statement: CompiledStatement,
    parameterValues: Map[String, Option[Any]]
  ): Select[T] = {
    copy(
      statement = statement,
      parameterValues = parameterValues
    )
  }
}

object Select {
  def apply[T](
    queryText: String,
    hasParameters: Boolean = true,
    queryOptions: QueryOptions = QueryOptions.default
  )(implicit converter: CRow => T
  ): Select[T] = {
    Select[T](
      statement = CompiledStatement(queryText, hasParameters),
      parameterValues = Map.empty[String, Option[ParameterValue]],
      queryOptions
    )
  }
}
