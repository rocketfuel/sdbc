package com.rocketfuel.sdbc.base.jdbc

import java.sql.{Types, PreparedStatement}
import com.rocketfuel.sdbc.base

trait ParameterValue
  extends base.ParameterValue
  with Index {
  self: Row =>

  override type Index = Int

  override type Statement = PreparedStatement

  override type Connection = java.sql.Connection

  override def prepareStatement(statement: String, connection: Connection): PreparedStatement = {
    connection.prepareStatement(statement)
  }

  override def setNone(preparedStatement: Statement, parameterIndex: Index): PreparedStatement = {
    preparedStatement.setNull(parameterIndex, Types.NULL)
    preparedStatement
  }

}
