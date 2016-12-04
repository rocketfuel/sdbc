package com.rocketfuel.sdbc.mariadb

import com.rocketfuel.sdbc.base.jdbc.Connection
import org.mariadb.jdbc.{MariaDbConnection => BaseMariaDbConnection}

trait MariaDbConnection extends Connection {
  override type BaseConnection = BaseMariaDbConnection

  override protected val baseConnection: Class[BaseMariaDbConnection] =
    classOf[BaseMariaDbConnection]

  override protected def toBaseConnection(connection: Connection): BaseConnection =
    connection.unwrap[BaseConnection](baseConnection)
}
