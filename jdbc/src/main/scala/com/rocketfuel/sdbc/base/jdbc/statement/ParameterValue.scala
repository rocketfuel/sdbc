package com.rocketfuel.sdbc.base.jdbc.statement

import com.rocketfuel.sdbc.base
import java.sql

trait ParameterValue
  extends base.ParameterValue
  with base.ParameterizedQuery {

  override type PreparedStatement = sql.PreparedStatement

  override def setNone(preparedStatement: PreparedStatement, parameterIndex: Int): PreparedStatement = {
    preparedStatement.setNull(parameterIndex, sql.Types.NULL)
    preparedStatement
  }

}
