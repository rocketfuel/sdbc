package com.rocketfuel.sdbc.cassandra.implementation

import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.{CompiledStatement, Logging}
import com.rocketfuel.sdbc.cassandra._
import scala.concurrent._
import scala.collection.convert.decorateAsScala._

trait Select {
  self: Cassandra =>

  case class Select[T] private [cassandra] (
    override val statement: CompiledStatement,
    override val parameterValues: Map[String, ParameterValue],
    override val queryOptions: QueryOptions
  )(implicit val converter: RowConverter[T])
    extends base.Select[Session, T]
    with ParameterizedQuery[Select[T]]
    with HasQueryOptions
    with Logging {

    override def iterator()(implicit session: Session): Iterator[T] = {
      logger.debug(s"""Selecting "$originalQueryText" with parameters $parameterValues.""")
      val prepared = prepare(this, queryOptions)
      session.execute(prepared).iterator.asScala.map(converter)
    }

    def iteratorAsync()(implicit session: Session, ec: ExecutionContext): Future[Iterator[T]] = {
      logger.debug(s"""Asynchronously selecting "$originalQueryText" with parameters $parameterValues.""")

      val prepared = prepare(this, queryOptions)
      val toListen = session.executeAsync(prepared)

      for {
        result <- implementation.toScalaFuture(toListen)
      } yield {
        result.iterator().asScala.map(converter)
      }
    }

    def option()(implicit session: Session): Option[T] = {
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
      parameterValues: Map[String, ParameterValue]
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
        parameterValues = Map.empty[String, ParameterValue],
        queryOptions
      )
    }
  }

}
