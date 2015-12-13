package com.rocketfuel.sdbc.base.jdbc

import java.sql.{Types, PreparedStatement}
import com.rocketfuel.sdbc.base

trait ParameterValue
  extends base.ParameterValue {
  self: Row =>

  override type ParameterIndex = Int

  override type Statement = PreparedStatement

  override type Connection = java.sql.Connection

  override def prepareStatement(statement: String, connection: Connection): PreparedStatement = {
    connection.prepareStatement(statement)
  }

  override def setNone(preparedStatement: Statement, parameterIndex: ParameterIndex): PreparedStatement = {
    preparedStatement.setNull(parameterIndex, Types.NULL)
    preparedStatement
  }

}
