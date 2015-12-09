package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.jdbc
import com.zaxxer.hikari.HikariDataSource

abstract class DBMS
  extends ParameterValue
  with HikariImplicits
  with base.ParameterizedQuery
  with Batch
  with Update
  with Select
  with SelectForUpdate
  with Execute
  with UpdaterImplicits
  with StringContextMethods
  with ResultSetImplicits
  with JdbcProcess
  with Getter
  with Updater
  with Row
  with MutableRow
  with ImmutableRow
  with UpdatableRow
  with Index
  with CompositeGetter
  with RowConverter {

  type Batchable[Key] = base.Batchable[Key, Connection, Batch]

  type Executable[Key] = base.Executable[Key, Connection, Execute]

  type Selectable[Key, Value] = base.Selectable[Key, Value, Connection, Select[Value]]

  type Updatable[Key] = base.Updatable[Key, Connection, Update]

  trait SelectForUpdatable[Key] {

    def select(key: Key): SelectForUpdatable[Key]

  }

  type Pool = jdbc.Pool

  val Pool = jdbc.Pool

  implicit def PoolToHikariPool(pool: Pool): HikariDataSource = {
    pool.underlying
  }

  implicit class ConnectionMethods(connection: Connection) {

    def iterator[T](
      queryText: String,
      parameters: (String, ParameterValue)*
    )(implicit converter: RowConverter[T]
    ): Iterator[T] = {
      Select[T](queryText).on(parameters: _*).iterator()(connection)
    }

    def iteratorForUpdate(
      queryText: String,
      parameters: (String, ParameterValue)*
    ): Iterator[UpdatableRow] = {
      SelectForUpdate(queryText).on(parameters: _*).iterator()(connection)
    }

    def option[T](
      queryText: String,
      parameters: (String, ParameterValue)*
    )(implicit converter: RowConverter[T]
    ): Option[T] = {
      Select[T](queryText).on(parameters: _*).option()(connection)
    }

    def update(
      queryText: String,
      parameterValues: (String, ParameterValue)*
    ): Long = {
      Update(queryText).on(parameterValues: _*).update()(connection)
    }

    def execute(
      queryText: String,
      parameterValues: (String, ParameterValue)*
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

  register(this)

}
