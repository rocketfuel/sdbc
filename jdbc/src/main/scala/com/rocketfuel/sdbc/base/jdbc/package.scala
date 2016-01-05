package com.rocketfuel.sdbc.base

import com.zaxxer.hikari.HikariConfig

package object jdbc
  extends HikariImplicits {

  type Connection = java.sql.Connection

  private val dataSources: collection.mutable.Map[String, DBMS] = collection.mutable.Map.empty

  private val jdbcSchemes: collection.mutable.Map[String, DBMS] = {
    import scala.collection.convert.decorateAsScala._
    //Scala's collections don't contain an ordered mutable map,
    //so just use java's.
    new java.util.TreeMap[String, DBMS](CaseInsensitiveOrdering).asScala
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
