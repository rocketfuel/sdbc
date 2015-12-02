package com.rocketfuel.sdbc.cassandra

import com.datastax.driver.core._
import com.rocketfuel.sdbc.{cassandra, base}
import com.rocketfuel.sdbc.base.{CompiledStatement, Logging}
import com.rocketfuel.sdbc.cassandra.implementation._

import scala.concurrent.{ExecutionContext, Future}

case class Execute private [cassandra] (
  override val statement: CompiledStatement,
  override val parameterValues: Map[String, Option[Any]],
  override val queryOptions: QueryOptions
) extends base.Execute[Session]
  with ParameterizedQuery[Execute]
  with HasQueryOptions
  with Logging {

  override def execute()(implicit session: Session): Unit = {
    logger.debug(s"""Executing "$originalQueryText" with parameters $parameterValues.""")
    val prepared = implementation.prepare(this, queryOptions)
    session.execute(prepared)
  }

  def executeAsync()(implicit session: Session, ec: ExecutionContext): Future[Unit] = {
    logger.debug(s"""Asynchronously executing "$originalQueryText" with parameters $parameterValues.""")
    val prepared = implementation.prepare(this, queryOptions)

    toScalaFuture[ResultSet](session.executeAsync(prepared)).map(Function.const(()))
  }

  override def subclassConstructor(
    statement: CompiledStatement,
    parameterValues: Map[String, Option[Any]]
  ): Execute = {
    copy(
      statement = statement,
      parameterValues = parameterValues
    )
  }
}

object Execute {
  def apply(
    queryText: String,
    hasParameters: Boolean = true,
    queryOptions: QueryOptions = cassandra.QueryOptions.default
  ): Execute = {
    Execute(
      statement = CompiledStatement(queryText, hasParameters),
      parameterValues = Map.empty[String, Option[ParameterValue]],
      queryOptions
    )
  }
}
