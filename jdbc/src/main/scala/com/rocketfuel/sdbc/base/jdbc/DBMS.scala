package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base
import com.zaxxer.hikari.HikariConfig

abstract class DBMS
  extends ParameterValue
  with HikariImplicits
  with Pool
  with Batch
  with Update
  with Select
  with SelectForUpdate
  with Execute
  with UpdaterImplicits
  with base.Selectable
  with base.Updatable
  with base.Batchable
  with base.Executable
  with StringContextMethods
  with ResultSetImplicits
  with JdbcProcess
  with Getter
  with Updater
  with Row
  with MutableRow
  with ImmutableRow
  with UpdatableRow
  with CompositeGetter
  with RowConverter {

  trait SelectForUpdatable[Key] {

    def select(key: Key): SelectForUpdatable[Key]

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

  DBMS.register(this)

}

object DBMS {

  private val dataSources: collection.mutable.Map[String, DBMS] = collection.mutable.Map.empty

  private val jdbcSchemes: collection.mutable.Map[String, DBMS] = {
    import scala.collection.convert.decorateAsScala._
    //Scala's collections don't contain an ordered mutable map,
    //so just use java's.
    new java.util.TreeMap[String, DBMS](base.CaseInsensitiveOrdering).asScala
  }

  private val productNames: collection.mutable.Map[String, DBMS] = collection.mutable.Map.empty

  private val jdbcURIRegex = "(?i)jdbc:(.+):.*".r

  private [jdbc] def register(dbms: DBMS): Unit = {
    this.synchronized {
      dataSources(dbms.dataSourceClassName) = dbms
      for (scheme <- dbms.jdbcSchemes) {
        jdbcSchemes(scheme) = dbms
      }
      productNames(dbms.productName) = dbms
      Class.forName(dbms.driverClassName)
    }
  }

  def ofConnectionString(connectionString: String): DBMS = {
    val jdbcURIRegex(scheme) = connectionString

    jdbcSchemes(scheme)
  }

  def ofDataSourceClassName(toLookup: String): DBMS = {
    dataSources(toLookup)
  }

  def of(config: HikariConfig): DBMS = {
    val dataSourceClassDbms = Option(config.getDataSourceClassName).flatMap(dataSources.get)
    val urlDbms = Option(config.getJdbcUrl).map(ofConnectionString)
    dataSourceClassDbms.
      orElse(urlDbms).
      get
  }

  def of(c: java.sql.Connection): DBMS = {
    productNames(c.getMetaData.getDatabaseProductName)
  }

  def of(s: java.sql.PreparedStatement): DBMS = {
    of(s.getConnection)
  }

  def of(s: java.sql.Statement): DBMS = {
    of(s.getConnection)
  }

  def of(r: java.sql.ResultSet): DBMS = {
    of(r.getStatement)
  }

}
