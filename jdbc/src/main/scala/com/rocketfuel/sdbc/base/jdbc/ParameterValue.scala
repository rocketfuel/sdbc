package com.rocketfuel.sdbc.base.jdbc

import java.sql.{Types, PreparedStatement}
import com.rocketfuel.sdbc.base

trait ParameterValue
  extends base.ParameterValue
  with base.ParameterizedQuery {

  override type Statement = PreparedStatement

  override type Connection = java.sql.Connection

  override def prepareStatement(statement: String)(implicit connection: Connection): PreparedStatement = {
    connection.prepareStatement(statement)
  }

  override def setNone(preparedStatement: Statement, parameterIndex: Int): PreparedStatement = {
    preparedStatement.setNull(parameterIndex, Types.NULL)
    preparedStatement
  }

}
