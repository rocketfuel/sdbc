package com.rocketfuel.sdbc.base.jdbc

import java.{sql, util}
import java.sql.{Array => _, _}
import java.util.Properties
import java.util.concurrent.Executor

trait Connection {
  self =>

  type BaseConnection

  protected val baseConnection: Class[BaseConnection]

  /*
  You might be wondering: Why not define an implicit conversion from Connection to BaseConnection, and call it good?
  Excellent question!

  The reason is that it would break the PostgreSql implementation. In that case, BaseConnection is
  org.postgresql.core.BaseConnection. However, when we close a connection, for it to be returned to the connection pool,
  we have to call com.zaxxer.hikari.pool.ProxyConnection#close(). When we call
  Connection#unwrap(classOf[BaseConnection]), we pass through ProxyConnection, so we lose ProxyConnection#close().

  This is how the class embedding looks:
  SDBC connection contains a Hikari connection contains a PostgreSql BaseConnection

  We also can't have separate implicit conversions to Connection and BaseConnection, because BaseConnection
  is also a Connection, and so they conflict.
   */
  class Connection(underlying: sql.Connection) extends sql.Connection {
    initializeConnection(this)

    override def setAutoCommit(autoCommit: Boolean): Unit = underlying.setAutoCommit(autoCommit)

    override def setHoldability(holdability: Int): Unit = underlying.setHoldability(holdability: Int)

    override def clearWarnings(): Unit = underlying.clearWarnings()

    override def getNetworkTimeout: Int = underlying.getNetworkTimeout

    override def createBlob(): Blob = underlying.createBlob()

    override def createSQLXML(): SQLXML = underlying.createSQLXML()

    override def setSavepoint(): Savepoint = underlying.setSavepoint()

    override def setSavepoint(name: String): Savepoint = underlying.setSavepoint(name: String)

    override def createNClob(): NClob = underlying.createNClob()

    override def getTransactionIsolation: Int = underlying.getTransactionIsolation

    override def getClientInfo(name: String): String = underlying.getClientInfo(name: String)

    override def getClientInfo: Properties = underlying.getClientInfo

    override def getSchema: String = underlying.getSchema

    override def setNetworkTimeout(executor: Executor, milliseconds: Int): Unit = underlying.setNetworkTimeout(executor: Executor, milliseconds: Int)

    override def getMetaData: DatabaseMetaData = underlying.getMetaData

    override def getTypeMap: util.Map[String, Class[_]] = underlying.getTypeMap

    override def rollback(): Unit = underlying.rollback()

    override def rollback(savepoint: Savepoint): Unit = underlying.rollback(savepoint: Savepoint)

    override def createStatement(): Statement = underlying.createStatement()

    override def createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement = underlying.createStatement(resultSetType: Int, resultSetConcurrency: Int)

    override def createStatement(
      resultSetType: Int,
      resultSetConcurrency: Int,
      resultSetHoldability: Int
    ): Statement = underlying.createStatement(
      resultSetType: Int,
      resultSetConcurrency: Int,
      resultSetHoldability: Int
    )

    override def getHoldability: Int = underlying.getHoldability

    override def setReadOnly(readOnly: Boolean): Unit = underlying.setReadOnly(readOnly: Boolean)

    override def setClientInfo(name: String, value: String): Unit = underlying.setClientInfo(name: String, value: String)

    override def setClientInfo(properties: Properties): Unit = underlying.setClientInfo(properties: Properties)

    override def isReadOnly: Boolean = underlying.isReadOnly: Boolean

    override def setTypeMap(map: util.Map[String, Class[_]]): Unit = underlying.setTypeMap(map: util.Map[String, Class[_]])

    override def getCatalog: String = underlying.getCatalog: String

    override def createClob(): Clob = underlying.createClob()

    override def setTransactionIsolation(level: Int): Unit = underlying.setTransactionIsolation(level: Int)

    override def nativeSQL(sql: String): String = underlying.nativeSQL(sql: String)

    override def prepareCall(sql: String): CallableStatement = underlying.prepareCall(sql: String)

    override def prepareCall(
      sql: String,
      resultSetType: Int,
      resultSetConcurrency: Int
    ): CallableStatement = underlying.prepareCall(
      sql: String,
      resultSetType: Int,
      resultSetConcurrency: Int
    )

    override def prepareCall(
      sql: String,
      resultSetType: Int,
      resultSetConcurrency: Int,
      resultSetHoldability: Int
    ): CallableStatement = underlying.prepareCall(
      sql: String,
      resultSetType: Int,
      resultSetConcurrency: Int,
      resultSetHoldability: Int
    )

    override def createArrayOf(typeName: String, elements: Array[AnyRef]): sql.Array = createArrayOf(typeName: String, elements: Array[AnyRef])

    override def setCatalog(catalog: String): Unit = underlying.setCatalog(catalog: String)

    override def close(): Unit = underlying.close()

    override def getAutoCommit: Boolean = underlying.getAutoCommit

    override def abort(executor: Executor): Unit = underlying.abort(executor: Executor)

    override def isValid(timeout: Int): Boolean = underlying.isValid(timeout: Int)

    override def prepareStatement(sql: String): PreparedStatement = underlying.prepareStatement(sql: String)

    override def prepareStatement(
      sql: String,
      resultSetType: Int,
      resultSetConcurrency: Int
    ): PreparedStatement = underlying.prepareStatement(
      sql: String,
      resultSetType: Int,
      resultSetConcurrency: Int
    )

    override def prepareStatement(
      sql: String,
      resultSetType: Int,
      resultSetConcurrency: Int,
      resultSetHoldability: Int
    ): PreparedStatement = underlying.prepareStatement(
      sql: String,
      resultSetType: Int,
      resultSetConcurrency: Int,
      resultSetHoldability: Int
    )

    override def prepareStatement(
      sql: String,
      autoGeneratedKeys: Int
    ): PreparedStatement = underlying.prepareStatement(
      sql: String,
      autoGeneratedKeys: Int
    )

    override def prepareStatement(
      sql: String,
      columnIndexes: Array[Int]
    ): PreparedStatement = underlying.prepareStatement(
      sql: String,
      columnIndexes: Array[Int]
    )

    override def prepareStatement(
      sql: String,
      columnNames: Array[String]
    ): PreparedStatement = underlying.prepareStatement(
      sql: String,
      columnNames: Array[String]
    )

    override def releaseSavepoint(savepoint: Savepoint): Unit = underlying.releaseSavepoint(savepoint: Savepoint)

    override def isClosed: Boolean = underlying.isClosed

    override def createStruct(
      typeName: String,
      attributes: Array[AnyRef]
    ): Struct = underlying.createStruct(
      typeName: String,
      attributes: Array[AnyRef]
    )

    override def getWarnings: SQLWarning = underlying.getWarnings

    override def setSchema(schema: String): Unit = underlying.setSchema(schema: String)

    override def commit(): Unit = underlying.commit()

    override def unwrap[T](iface: Class[T]): T = {
      if (iface.isInstance(underlying))
        underlying.asInstanceOf[T]
      else underlying.unwrap[T](iface)
    }

    override def isWrapperFor(iface: Class[_]): Boolean = {
      iface.isInstance(underlying) ||
        underlying.isWrapperFor(iface)
    }
  }

  object Connection {

    implicit def toBaseConnection(connection: Connection): BaseConnection = {
      self.toBaseConnection(connection)
    }

    implicit def fromConnection(implicit connection: sql.Connection): Connection =
      new Connection(connection)

  }

  /**
    * Perform any connection initialization that should be done when a connection
    * is created. EG add a type mapping.
    *
    * By default this method does nothing.
    *
    * @param connection
    */
  protected def initializeConnection(connection: Connection): Unit = ()

  protected def toBaseConnection(connection: Connection): BaseConnection

}

trait JdbcConnection
  extends Connection {

  override type BaseConnection = sql.Connection

  override protected val baseConnection: Class[sql.Connection] =
    classOf[sql.Connection]

  override protected def toBaseConnection(connection: Connection): BaseConnection =
    connection

}
