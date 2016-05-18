package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.jdbc.resultset._
import com.rocketfuel.sdbc.base.jdbc.statement.{ParameterValue, StatementConverter}
import com.zaxxer.hikari.HikariConfig
import java.sql

abstract class DBMS
  extends ParameterValue
  with HikariImplicits
  with Pool
  with Select
  with SelectForUpdate
  with Execute
  with Update
  with Queryable
  with Batch
  with StringContextMethods
  with ResultSetImplicits
  with StatementConverter
  with Getter
  with Updater
  with Row
  with ImmutableRow
  with UpdatableRow
  with CompositeGetter
  with RowConverter
  with QueryMethods {

  type Singleton[A] = base.Singleton[A]

  val Singleton = base.Singleton

  type CloseableIterator[+A] = base.CloseableIterator[A]

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
    *
    * @param connection
   */
  def initializeConnection(connection: java.sql.Connection): Unit = {

  }

  type Statement = sql.PreparedStatement

  type CallableStatement = sql.CallableStatement

  type Blob = sql.Blob

  type SQLXML = sql.SQLXML

  type Savepoint = sql.Savepoint

  type Clob = sql.Clob

  type NClob = sql.NClob

  type DatabaseMetaData = sql.DatabaseMetaData

  type SQLWarning = sql.SQLWarning

  type Struct = sql.Struct

  DBMS.register(this)

}

object DBMS {

  private val dataSources: collection.mutable.Map[String, DBMS] = collection.concurrent.TrieMap.empty[String, DBMS]

  private val jdbcSchemes: collection.mutable.Map[String, DBMS] = {
    import scala.collection.convert.decorateAsScala._
    //Scala's collections don't contain an ordered mutable map,
    //so just use java's.
    new java.util.concurrent.ConcurrentSkipListMap[String, DBMS](String.CASE_INSENSITIVE_ORDER).asScala
  }

  private val productNames: collection.mutable.Map[String, DBMS] = collection.mutable.Map.empty

  private val jdbcURIRegex = "(?i)^jdbc:(.+):".r

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
