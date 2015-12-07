package com.rocketfuel.sdbc.base.jdbc

import java.sql.{Types, PreparedStatement}

import com.rocketfuel.sdbc.base

trait ParameterValue
  extends base.ParameterValue
  with base.ParameterSetter
  with Index {
  self: Row =>

  override type Index = Int

  override type Statement = PreparedStatement

  private [jdbc] def prepare(
    queryText: String,
    parameterValues: Map[String, Option[Any]],
    parameterPositions: Map[String, Set[Int]]
  )(implicit connection: Connection
  ): PreparedStatement = {
    val preparedStatement = connection.prepareStatement(queryText)

    bind(preparedStatement, parameterValues, parameterPositions)

    preparedStatement
  }

  private [jdbc] def bind(
    preparedStatement: PreparedStatement,
    parameterValues: Map[String, Option[Any]],
    parameterPositions: Map[String, Set[Int]]
  ): Unit = {
    for ((key, maybeValue) <- parameterValues) {
      val setter: Int => Unit = {
        maybeValue match {
          case None =>
            (parameterIndex: Int) =>
              setNone(preparedStatement, parameterIndex + 1)
          case Some(value) =>
            (parameterIndex: Int) =>
              setAny(preparedStatement, parameterIndex + 1, value)
        }
      }
      val parameterIndices = parameterPositions(key)
      parameterIndices.foreach(setter)
    }
  }

  override def setNone(preparedStatement: Statement, parameterIndex: Index): Unit = {
    preparedStatement.setNull(parameterIndex, Types.NULL)
  }

}
