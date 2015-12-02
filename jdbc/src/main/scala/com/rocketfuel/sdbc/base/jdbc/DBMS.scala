package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.jdbc
import com.zaxxer.hikari.HikariDataSource
import scalaz.stream._

abstract class DBMS
  extends HikariImplicits
  with base.ParameterValueImplicits
  with UpdaterImplicits
  with base.BatchableMethods[java.sql.Connection, Batch]
  with base.UpdatableMethods[java.sql.Connection, Update]
  with base.SelectableMethods[java.sql.Connection, Select]
  with base.ExecutableMethods[java.sql.Connection, Execute]
  with StringContextMethods {

  implicit val ParameterSetter: ParameterSetter

  type CompositeGetter[A] = jdbc.CompositeGetter[A]

  type Index = jdbc.Index

  type Getter[+T] = jdbc.Getter[T]

  type RowConverter[T] = jdbc.RowConverter[T]
  val RowConverter = jdbc.RowConverter

  type ParameterValue = jdbc.ParameterValue
  val ParameterValue = jdbc.ParameterValue

  type Batchable[Key] = base.Batchable[Key, Connection, Batch]

  type Executable[Key] = jdbc.Executable[Key]

  type Selectable[Key, Value] = jdbc.Selectable[Key, Value]

  type Updatable[Key] = jdbc.Updatable[Key]

  type Select[T] = jdbc.Select[T]

  object Select {
    def apply[T](
      queryText: String,
      hasParameters: Boolean = true
    )(implicit converter: RowConverter[T]
    ): Select[T] = {
      jdbc.Select[T](queryText, hasParameters)
    }
  }

  type SelectForUpdate = jdbc.SelectForUpdate

  object SelectForUpdate {
    def apply(
      queryText: String,
      hasParameters: Boolean = true
    ): SelectForUpdate = {
      jdbc.SelectForUpdate(queryText, hasParameters)
    }
  }

  type Update = jdbc.Update

  object Update {
    def apply(
      queryText: String,
      hasParameters: Boolean = true
    ): Update = {
      jdbc.Update(queryText, hasParameters)
    }
  }

  type Batch = jdbc.Batch

  object Batch {
    def apply(
      queryText: String,
      hasParameters: Boolean = true
    ): Batch = {
      jdbc.Batch(queryText, hasParameters)
    }
  }

  type Execute = jdbc.Execute

  object Execute {
    def apply(
      queryText: String,
      hasParameters: Boolean = true
    ): Execute = {
      jdbc.Execute(queryText, hasParameters)
    }
  }

  type Pool = jdbc.Pool

  val Pool = jdbc.Pool

  type Connection = jdbc.Connection

  type Row = jdbc.Row

  type MutableRow = jdbc.MutableRow

  type UpdatableRow = jdbc.UpdatableRow

  type ImmutableRow = jdbc.ImmutableRow

  implicit def PoolToHikariPool(pool: Pool): HikariDataSource = {
    pool.underlying
  }

  implicit class ConnectionMethods(connection: Connection) {

    def iterator[T](
      queryText: String,
      parameters: (String, Option[ParameterValue])*
    )(implicit converter: RowConverter[T]
    ): Iterator[T] = {
      Select[T](queryText).on(parameters: _*).iterator()(connection)
    }

    def iteratorForUpdate(
      queryText: String,
      parameters: (String, Option[ParameterValue])*
    ): Iterator[UpdatableRow] = {
      SelectForUpdate(queryText).on(parameters: _*).iterator()(connection)
    }

    def option[T](
      queryText: String,
      parameters: (String, Option[ParameterValue])*
    )(implicit converter: RowConverter[T]
    ): Option[T] = {
      Select[T](queryText).on(parameters: _*).option()(connection)
    }

    def update(
      queryText: String,
      parameterValues: (String, Option[ParameterValue])*
    ): Long = {
      Update(queryText).on(parameterValues: _*).update()(connection)
    }

    def execute(
      queryText: String,
      parameterValues: (String, Option[ParameterValue])*
    ): Unit = {
      Execute(queryText).on(parameterValues: _*).execute()(connection)
    }

  }

  /**
   * Class name for the DataSource class.
   */
  def dataSourceClassName: String

  /**
   * Class name for the JDBC driver class.
   */
  def driverClassName: String

  def jdbcSchemes: Set[String]

  /**
   * The result of getMetaData.getDatabaseProductName
   */
  def productName: String

  /**
   * If the JDBC driver supports the .isValid() method.
   */
  def supportsIsValid: Boolean

  /**
   * Perform any connection initialization that should be done when a connection
   * is created. EG add a type mapping.
   *
   * By default this method does nothing.
   * @param connection
   */
  def initializeConnection(connection: java.sql.Connection): Unit = {

  }

  val JdbcProcess = jdbc.JdbcProcess

  object HasJdbcProcess {
    val jdbc = JdbcProcess
  }

  implicit def ProcessToHasJdbcProcess(x: Process.type): HasJdbcProcess.type = {
    HasJdbcProcess
  }

  register(this)

}
