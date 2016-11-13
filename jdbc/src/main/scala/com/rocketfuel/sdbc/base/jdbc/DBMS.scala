package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.jdbc.resultset._
import com.rocketfuel.sdbc.base.jdbc.statement.{ParameterValue, StatementConverter}
import java.sql

abstract class DBMS
  extends ParameterValue
  with HikariImplicits
  with Pool
  with Select
  with SelectForUpdate
  with Execute
  with Update
  with Batch
  with Selectable
  with SelectForUpdatable
  with Updatable
  with Batchable
  with Executable
  with StringContextMethods
  with ResultSetImplicits
  with StatementConverter
  with Getter
  with Updater
  with Row
  with ImmutableRow
  with ConnectedRow
  with CompositeGetter
  with RowConverter
  with QueryMethods
  with StreamSupport {
  self: com.rocketfuel.sdbc.base.jdbc.Connection =>

  type CloseableIterator[+A] = base.CloseableIterator[A]

  type CompiledStatement = base.CompiledStatement

  val CompiledStatement = base.CompiledStatement

  /**
   * Override if the driver does not support .isValid().
   */
  def connectionTestQuery: Option[String] = None

  protected val supportsGetLargeUpdateCount: Boolean = true

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

}
