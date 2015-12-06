package com.rocketfuel.sdbc.base.jdbc

import java.sql._
import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.{Logging, CompiledStatement}

trait SelectForUpdate {
  self: ParameterValue
    with base.CompositeSetter
    with base.ParameterizedQuery
    with UpdatableRow
    with Updater
    with MutableRow
    with ResultSetImplicits =>

  case class SelectForUpdate private[sdbc](
    statement: CompiledStatement,
    parameterValues: Map[String, Option[Any]]
  ) extends base.Select[Connection, UpdatableRow]
  with ParameterizedQuery[SelectForUpdate]
  with Logging {

    private def executeQuery()(implicit connection: Connection): ResultSet = {
      logger.debug( s"""Selecting for update "$originalQueryText" with parameters $parameterValues.""")
      val preparedStatement = connection.prepareStatement(queryText, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
      bind(preparedStatement, parameterValues, parameterPositions)

      preparedStatement.executeQuery()
    }

    override def iterator()(implicit connection: Connection): Iterator[UpdatableRow] = {
      executeQuery().updatableIterator()
    }

    override protected def subclassConstructor(
      statement: CompiledStatement,
      parameterValues: Map[String, Option[Any]]
    ): SelectForUpdate = {
      SelectForUpdate(
        statement,
        parameterValues
      )
    }
  }

  object SelectForUpdate {
    def apply(
      queryText: String,
      hasParameters: Boolean = true
    ): SelectForUpdate = {
      SelectForUpdate(
        statement = CompiledStatement(queryText, hasParameters),
        parameterValues = Map.empty[String, Option[Any]]
      )
    }
  }

}
