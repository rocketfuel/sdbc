package com.rocketfuel.sdbc.postgresql.implementation

import java.sql.Connection

import org.postgresql.PGConnection

private[sdbc] trait ConnectionImplicits {

  /** This can be used to get to the getCopyApi() and other methods.
    * @param connection The Connection or Hikari Connection which contains an underlying PGConnection.
    * @return The underlying PGConnection.
    */
  implicit def PostgreSqlConnectionToPGConnection(connection: Connection): PGConnection = {
    connection.unwrap(classOf[PGConnection])
  }

}
