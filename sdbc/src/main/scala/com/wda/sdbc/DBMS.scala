package com.wda.sdbc

import com.wda.CaseInsensitiveOrdering
import com.zaxxer.hikari.HikariConfig
import scala.collection.immutable.Seq
import base._

abstract class DBMS
  extends Pool
  with Connection
  with AbstractQuery
  with Update
  with Select
  with SelectForUpdate
  with Batch
  with Getter
  with Row
  with GetterImplicits
  with AbstractDeployable
  with Resources
  with ParameterValue {
  self =>

  /**
   * Class name for the DataSource class.
   */
  def dataSourceClassName: String

  /**
   * Class name for the JDBC driver class.
   */
  def driverClassName: String

  def jdbcScheme: String

  /**
   * The result of getMetaData.getDatabaseProductName
   */
  def productName: String

  /**
   * If the JDBC driver supports the .isValid() method.
   */
  def supportsIsValid: Boolean

  def Identifier: Identifier

  /**
   * Perform any connection initialization that should be done when a connection
   * is created. EG add a type mapping.
   *
   * By default this method does nothing.
   * @param connection
   */
  def initializeConnection(connection: java.sql.Connection): Unit = {

  }

  /**
   * Utilities for use by buildInsert.
   */
  protected object QueryBuilder {

    def columnNames(columnOrder: Seq[String], defaults: Set[String]): String = {
      columnOrder.filter(c => ! defaults.contains(c)).map(Identifier.quote).mkString("(", ",", ")")
    }

    def columnValues(columnOrder: Seq[String], defaults: Set[String]): String = {
      columnOrder.filter(c => ! defaults.contains(c)).map("$`" + _ + "`").mkString("(", ",", ")")
    }

  }

  /**
   * Creates an insert statement that returns all the values that were inserted.
   * @param tableSchema
   * @param tableName
   * @param columnOrder
   * @param defaults The columns that are to be inserted with default values.
   * @param conversion
   * @tparam T
   * @return
   */
  def buildInsert[T](
    tableSchema: String,
    tableName: String,
    columnOrder: Seq[String],
    defaults: Set[String]
  )(implicit conversion: Row => T
  ): Select[T]

  DBMS.register(this)

}

object DBMS {

  private val dataSources: collection.mutable.Map[String, DBMS] = collection.mutable.Map.empty

  private val jdbcSchemes: collection.mutable.Map[String, DBMS] = {
    import scala.collection.convert.decorateAsScala._
    //Scala's collections don't contain an ordered mutable map,
    //so just use java's.
    new java.util.TreeMap[String, DBMS](CaseInsensitiveOrdering).asScala
  }

  private val productNames: collection.mutable.Map[String, DBMS] = collection.mutable.Map.empty

  private val jdbcURIRegex = "jdbc:(.+)://.+".r

  def register(dbms: DBMS): Unit = {
    this.synchronized {
      dataSources(dbms.dataSourceClassName) = dbms
      jdbcSchemes(dbms.jdbcScheme) = dbms
      productNames(dbms.productName) = dbms
      Class.forName(dbms.driverClassName)
    }
  }

  def ofJdbcUrl(connectionString: String): DBMS = {
    val jdbcURIRegex(scheme) = connectionString

    jdbcSchemes(scheme)
  }

  def ofDataSourceClassName(toLookup: String): DBMS = {
    dataSources(toLookup)
  }

  def of(config: HikariConfig): DBMS = {
    val dataSourceClassDbms = Option(config.getDataSourceClassName).flatMap(dataSources.get)
    val urlDbms = Option(config.getJdbcUrl).map(ofJdbcUrl)
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

  def wrap(c: java.sql.Connection): DBMS#Connection = {
    of(c).Connection(c)
  }

}
