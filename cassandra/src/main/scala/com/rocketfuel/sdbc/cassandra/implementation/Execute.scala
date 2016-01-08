package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core
import com.rocketfuel.sdbc.base.{CompiledStatement, Logging}
import com.rocketfuel.sdbc.cassandra._
import com.rocketfuel.sdbc.{base, cassandra}
import scala.concurrent.{ExecutionContext, Future}

trait Execute {
  self: Cassandra =>

  case class Execute private [cassandra] (
    override val statement: CompiledStatement,
    override val parameterValues: Map[String, ParameterValue],
    override val queryOptions: QueryOptions
  ) extends base.Execute[Session]
  with ParameterizedQuery[Execute]
  with HasQueryOptions
  with Logging {

    override def execute()(implicit session: Session): Unit = {
      logger.debug(s"""Executing "$originalQueryText" with parameters $parameterValues.""")
      val prepared = prepare(this, queryOptions)
      session.execute(prepared)
    }

    def executeAsync()(implicit session: Session, ec: ExecutionContext): Future[Unit] = {
      logger.debug(s"""Asynchronously executing "$originalQueryText" with parameters $parameterValues.""")
      val prepared = prepare(this, queryOptions)

      toScalaFuture[core.ResultSet](session.executeAsync(prepared)).map(Function.const(()))
    }

    override def subclassConstructor(
      statement: CompiledStatement,
      parameterValues: Map[String, ParameterValue]
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
        parameterValues = Map.empty[String, ParameterValue],
        queryOptions
      )
    }
  }

}
