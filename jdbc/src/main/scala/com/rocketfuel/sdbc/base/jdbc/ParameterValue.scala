package com.rocketfuel.sdbc.base.jdbc

import java.sql
import com.rocketfuel.sdbc.base

trait ParameterValue
  extends base.ParameterValue
  with base.ParameterizedQuery {
    self: base.jdbc.Connection =>

  override type PreparedStatement = sql.PreparedStatement

  override def prepareStatement(statement: String)(implicit connection: Connection): PreparedStatement = {
    connection.prepareStatement(statement)
  }

  override def setNone(preparedStatement: PreparedStatement, parameterIndex: Int): PreparedStatement = {
    preparedStatement.setNull(parameterIndex, sql.Types.NULL)
    preparedStatement
  }

}
