package com.rocketfuel.sdbc.postgresql.implementation

import com.rocketfuel.sdbc.base.jdbc.Connection
import com.rocketfuel.sdbc.postgresql.{Cidr, LTree}

trait PgConnection
  extends Connection {

  override type BaseConnection = org.postgresql.core.BaseConnection

  override protected val baseConnection: Class[BaseConnection] =
    classOf[BaseConnection]

  /**
    * Perform any connection initialization that should be done when a connection
    * is created. EG add a type mapping.
    *
    * By default this method does nothing.
    *
    * @param connection
    */
  override protected def initializeConnection(connection: Connection): Unit = {
    connection.addDataType("ltree", classOf[LTree])
    connection.addDataType("inet", classOf[PGInetAddress])
    connection.addDataType("cidr", classOf[Cidr])
    connection.addDataType("json", classOf[PGJson])
    connection.addDataType("jsonb", classOf[PGJson])
    //The PG driver won't use these registered custom classes, even if we register them.
    //    pgConnection.addDataType("time", classOf[PGLocalTime])
    //    pgConnection.addDataType("timetz", classOf[PGTimeTz])
    //    pgConnection.addDataType("timestamptz", classOf[PGTimestampTz])
  }

  override protected def toBaseConnection(connection: Connection): BaseConnection = {
    connection.unwrap[org.postgresql.core.BaseConnection](baseConnection)
  }

}
