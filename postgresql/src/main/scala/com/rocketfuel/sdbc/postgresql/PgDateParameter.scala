package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.base.jdbc.statement.{DateParameter, ParameterValue}
import java.time.LocalTime

trait PgDateParameter extends DateParameter {
  self: ParameterValue =>

  override implicit object LocalTimeParameter extends Parameter[LocalTime] {
    override val set: LocalTime => (PreparedStatement, Int) => PreparedStatement = {
      time => (statement, ix) =>
        val pgTime = PGLocalTime(time)
        statement.setObject(ix + 1, pgTime)
        statement
    }
  }

}

