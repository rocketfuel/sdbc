package com.rocketfuel.sdbc.cassandra.datastax

import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.{Logging, CompiledStatement}
import com.datastax.driver.core.{Row => CRow, _}
import com.rocketfuel.sdbc.cassandra.datastax.implementation.RowConverter
import scala.concurrent._
import scala.collection.convert.decorateAsScala._

case class Select[T] private [cassandra] (
  override val statement: CompiledStatement,
  override val parameterValues: Map[String, Option[Any]],
  override val queryOptions: QueryOptions
)(implicit val converter: implementation.RowConverter[T])
  extends base.Select[Session, T]
  with implementation.ParameterizedQuery[Select[T]]
  with implementation.HasQueryOptions
  with Logging {

  override def iterator()(implicit session: Session): Iterator[T] = {
    logger.debug(s"""Selecting "$originalQueryText" with parameters $parameterValues.""")
    val prepared = implementation.prepare(statement, parameterValues, queryOptions)
    session.execute(prepared).iterator.asScala.map(converter)
  }

  def iteratorAsync()(implicit session: Session, ec: ExecutionContext): Future[Iterator[T]] = {
    logger.debug(s"""Asynchronously selecting "$originalQueryText" with parameters $parameterValues.""")

    val prepared = implementation.prepare(statement, parameterValues, queryOptions)
    val toListen = session.executeAsync(prepared)

    for {
      result <- implementation.toScalaFuture(toListen)
    } yield {
      result.iterator().asScala.map(converter)
    }
  }

  override def option()(implicit session: Session): Option[T] = {
    iterator().toStream.headOption
  }

  def optionAsync()(implicit session: Session, ec: ExecutionContext): Future[Option[T]] = {
    for {
      result <- iteratorAsync()
    } yield {
      result.toStream.headOption
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
  )(implicit converter: RowConverter[T]
  ): Select[T] = {
    Select[T](
      statement = CompiledStatement(queryText, hasParameters),
      parameterValues = Map.empty[String, Option[ParameterValue]],
      queryOptions
    )
  }
}
