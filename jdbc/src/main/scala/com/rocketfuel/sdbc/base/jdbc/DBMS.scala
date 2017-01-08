package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.{Getter, RowConverter}
import com.rocketfuel.sdbc.base.jdbc.resultset._
import com.rocketfuel.sdbc.base.jdbc.statement.{ParameterValue, StatementConverter}
import java.sql

trait DBMS
  extends ParameterValue
  with HikariImplicits
  with Pool
  with Select
  with SelectForUpdate
  with Ignore
  with Update
  with Batch
  with Insert
  with Delete
  with Selectable
  with SelectForUpdatable
  with Updatable
  with Batchable
  with Ignorable
  with Insertable
  with Deletable
  with StringContextMethods
  with ResultSetImplicits
  with StatementConverter
  with Getter
  with Updater
  with ImmutableRow
  with ConnectedRow
  with RowConverter
  with QueryMethods
  with StreamUtils {
  self: com.rocketfuel.sdbc.base.jdbc.Connection =>

  type CloseableIterator[+A] = base.CloseableIterator[A]

  type CompiledStatement = base.CompiledStatement

  val CompiledStatement = base.CompiledStatement

  type Index = base.Index
  val Index = base.Index

  /**
   * Override if the driver does not support .isValid().
   */
  def connectionTestQuery: Option[String] = None

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

  type Row = ConnectedRow

  type ResultSetIterator = resultset.ResultSetIterator

  protected def ofVal[T <: AnyVal](valGetter: (ConnectedRow, Int) => T): Getter[T] = {
    (row: ConnectedRow, ix: Int) =>
      val value = valGetter(row, ix)
      if (row.wasNull) None
      else Some(value)
  }

  trait syntax
    extends Batch.syntax
      with Deletable.Partable
      with Ignorable.Partable
      with Insertable.Partable
      with Selectable.Partable
      with SelectForUpdatable.Partable
      with Updatable.Partable
      with Batchable.syntax
      with Ignorable.syntax
      with Insertable.syntax
      with Selectable.syntax
      with SelectForUpdatable.syntax
      with Updatable.syntax
      with Deletable.syntax

  val syntax = new syntax {}

}
