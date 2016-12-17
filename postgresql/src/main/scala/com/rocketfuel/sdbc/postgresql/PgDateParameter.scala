package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.base.jdbc.statement.{DateParameter, ParameterValue}
import java.time.LocalTime

trait PgDateParameter extends DateParameter {
  self: ParameterValue =>

  override implicit val LocalTimeParameter: Parameter[LocalTime] =
    (time: LocalTime) => {
      val pgTime = PGLocalTime(time)
      (statement: PreparedStatement, ix: Int) => {
        statement.setObject(ix + 1, pgTime)
        statement
      }
    }

}
