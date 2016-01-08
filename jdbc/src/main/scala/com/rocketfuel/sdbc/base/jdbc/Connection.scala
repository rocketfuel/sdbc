package com.rocketfuel.sdbc.base.jdbc

import java.sql
import java.util
import java.util.concurrent.Executor

trait Connection {
  self: DBMS =>

  type Statement = sql.Statement

  type CallableStatement = sql.CallableStatement

  type Blob = sql.Blob

  type SQLXML = sql.SQLXML

  type Savepoint = sql.Savepoint

  type Clob = sql.Clob

  type NClob = sql.NClob

  type DatabaseMetaData = sql.DatabaseMetaData

  type SQLWarning = sql.SQLWarning

  type Struct = sql.Struct

  case class Connection(underlying: sql.Connection) extends java.sql.Connection {
    override def unwrap[T](iface: Class[T]): T = {
      if (iface.isInstance(underlying)) {
        underlying.asInstanceOf[T]
      } else {
        underlying.unwrap[T](iface)
      }
    }

    override def isWrapperFor(iface: Class[_]): Boolean = {
      iface.isInstance(underlying) || underlying.isWrapperFor(iface)
    }

    def iterator[T](
      queryText: String,
      parameters: (String, ParameterValue)*
    )(implicit converter: RowConverter[T]
    ): Iterator[T] = {
      Select[T](queryText).on(parameters: _*).iterator()(this)
    }

    def iteratorForUpdate(
      queryText: String,
      parameters: (String, ParameterValue)*
    ): Iterator[UpdatableRow] = {
      SelectForUpdate(queryText).on(parameters: _*).iterator()(this)
    }

    def option[T](
      queryText: String,
      parameters: (String, ParameterValue)*
    )(implicit converter: RowConverter[T]
    ): Option[T] = {
      Select[T](queryText).on(parameters: _*).option()(this)
    }

    def update(
      queryText: String,
      parameterValues: (String, ParameterValue)*
    ): Long = {
      Update(queryText).on(parameterValues: _*).update()(this)
    }

    def batch(
      queryText: String,
      batches: Map[String, ParameterValue]*
    ): IndexedSeq[Long] = {
      Batch(queryText).copy(parameterValueBatches = batches).seq()
    }

    def execute(
      queryText: String,
      parameterValues: (String, ParameterValue)*
    ): Unit = {
      Execute(queryText).on(parameterValues: _*).execute()(this)
    }

    override def setAutoCommit(autoCommit: Boolean): Unit = underlying.setAutoCommit(autoCommit)

    override def setHoldability(holdability: Int): Unit = underlying.setHoldability(holdability)

    override def clearWarnings(): Unit = underlying.clearWarnings()

    override def getNetworkTimeout: Int = underlying.getNetworkTimeout

    override def createBlob(): Blob = underlying.createBlob()

    override def createSQLXML(): SQLXML = underlying.createSQLXML()

    override def setSavepoint(): Savepoint = underlying.setSavepoint()

    override def setSavepoint(name: String): Savepoint = underlying.setSavepoint(name)

    override def createNClob(): NClob = underlying.createNClob()

    override def getTransactionIsolation: Int = underlying.getTransactionIsolation

    override def getClientInfo(name: String): String = underlying.getClientInfo(name)

    override def getClientInfo: util.Properties = underlying.getClientInfo

    override def getSchema: String = underlying.getSchema

    override def setNetworkTimeout(executor: Executor, milliseconds: Int): Unit = underlying.setNetworkTimeout(executor, milliseconds)

    override def getMetaData: DatabaseMetaData = underlying.getMetaData

    override def getTypeMap: util.Map[String, Class[_]] = underlying.getTypeMap

    override def rollback(): Unit = underlying.rollback()

    override def rollback(savepoint: Savepoint): Unit = underlying.rollback(savepoint)

    override def createStatement(): Statement = underlying.createStatement()

    override def createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement =
      underlying.createStatement(resultSetType, resultSetConcurrency)

    override def createStatement(
      resultSetType: Int,
      resultSetConcurrency: Int,
      resultSetHoldability: Int
    ): Statement =
      underlying.createStatement(
        resultSetType,
        resultSetConcurrency,
        resultSetHoldability
      )

    override def getHoldability: Int = underlying.getHoldability

    override def setReadOnly(readOnly: Boolean): Unit = underlying.setReadOnly(readOnly)

    override def setClientInfo(name: String, value: String): Unit = underlying.setClientInfo(name, value)

    override def setClientInfo(properties: util.Properties): Unit = underlying.setClientInfo(properties)

    override def isReadOnly: Boolean = underlying.isReadOnly

    override def setTypeMap(map: util.Map[String, Class[_]]): Unit = underlying.setTypeMap(map)

    override def getCatalog: String = underlying.getCatalog()

    override def createClob(): Clob = underlying.createClob()

    override def setTransactionIsolation(level: Int): Unit = underlying.setTransactionIsolation(level)

    override def nativeSQL(sql: String): String = underlying.nativeSQL(sql)

    override def prepareCall(sql: String): CallableStatement = underlying.prepareCall(sql)

    override def prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int): CallableStatement =
    underlying.prepareCall(sql, resultSetType, resultSetConcurrency)

    override def prepareCall(
      sql: String,
      resultSetType: Int,
      resultSetConcurrency: Int,
      resultSetHoldability: Int
    ): CallableStatement =
      prepareCall(
        sql,
        resultSetType,
        resultSetConcurrency,
        resultSetHoldability
      )

    override def createArrayOf(typeName: String, elements: Array[AnyRef]): sql.Array =
      underlying.createArrayOf(typeName, elements)

    override def setCatalog(catalog: String): Unit = underlying.setCatalog(catalog)

    override def close(): Unit = underlying.close()

    override def getAutoCommit: Boolean = underlying.getAutoCommit

    override def abort(executor: Executor): Unit = underlying.abort(executor)

    override def isValid(timeout: Int): Boolean = underlying.isValid(timeout)

    override def prepareStatement(sql: String): PreparedStatement = underlying.prepareStatement(sql)

    override def prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement =
      underlying.prepareStatement(
        sql,
        resultSetType,
        resultSetConcurrency
      )

    override def prepareStatement(
      sql: String,
      resultSetType: Int,
      resultSetConcurrency: Int,
      resultSetHoldability: Int
    ): PreparedStatement =
      underlying.prepareStatement(
        sql,
        resultSetType,
        resultSetConcurrency,
        resultSetHoldability
      )

    override def prepareStatement(sql: String, autoGeneratedKeys: Int): PreparedStatement =
      underlying.prepareStatement(sql, autoGeneratedKeys)

    override def prepareStatement(sql: String, columnIndexes: Array[Int]): PreparedStatement =
      underlying.prepareStatement(sql, columnIndexes)

    override def prepareStatement(sql: String, columnNames: Array[String]): PreparedStatement =
      underlying.prepareStatement(sql, columnNames)

    override def releaseSavepoint(savepoint: Savepoint): Unit =
      underlying.releaseSavepoint(savepoint)

    override def isClosed: Boolean = underlying.isClosed

    override def createStruct(typeName: String, attributes: Array[AnyRef]): Struct =
      underlying.createStruct(typeName, attributes)

    override def getWarnings: SQLWarning = underlying.getWarnings

    override def setSchema(schema: String): Unit = underlying.getSchema

    override def commit(): Unit = underlying.commit()
  }

  object Connection {
    implicit def of(underlying: sql.Connection): Connection = {
      Connection(underlying)
    }
  }

}
