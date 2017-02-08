package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.base.jdbc.Connection

trait PgConnection
  extends Connection {

  def initializeJson(connection: Connection): Unit

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
    initializeJson(connection)
    //The PG driver won't use these registered custom classes, even if we register them.
    //    pgConnection.addDataType("time", classOf[PGLocalTime])
    //    pgConnection.addDataType("timetz", classOf[PGTimeTz])
    //    pgConnection.addDataType("timestamptz", classOf[PGTimestampTz])
  }

  override protected def toBaseConnection(connection: Connection): BaseConnection = {
    connection.unwrap[org.postgresql.core.BaseConnection](baseConnection)
  }

}
