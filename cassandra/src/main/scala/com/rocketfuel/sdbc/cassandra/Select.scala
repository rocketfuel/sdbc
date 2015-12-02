package com.rocketfuel.sdbc.cassandra

import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.{CompiledStatement, Logging}
import com.rocketfuel.sdbc.cassandra.implementation.RowConverter
import scala.concurrent._
import scala.collection.convert.decorateAsScala._

case class Select[T] private [cassandra] (
  override val statement: CompiledStatement,
  override val parameterValues: Map[String, Option[Any]],
  override val queryOptions: QueryOptions
)(implicit val converter: com.rocketfuel.sdbc.cassandra.implementation.RowConverter[T])
  extends base.Select[Session, T]
  with com.rocketfuel.sdbc.cassandra.implementation.ParameterizedQuery[Select[T]]
  with com.rocketfuel.sdbc.cassandra.implementation.HasQueryOptions
  with Logging {

  override def iterator()(implicit session: Session): Iterator[T] = {
    logger.debug(s"""Selecting "$originalQueryText" with parameters $parameterValues.""")
    val prepared = implementation.prepare(this, queryOptions)
    session.execute(prepared).iterator.asScala.map(converter)
  }

  def iteratorAsync()(implicit session: Session, ec: ExecutionContext): Future[Iterator[T]] = {
    logger.debug(s"""Asynchronously selecting "$originalQueryText" with parameters $parameterValues.""")

    val prepared = implementation.prepare(this, queryOptions)
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
