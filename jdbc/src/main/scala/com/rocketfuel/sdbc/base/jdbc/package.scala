package com.rocketfuel.sdbc.base

import java.sql.PreparedStatement
import com.rocketfuel.sdbc.base
import com.zaxxer.hikari.HikariConfig

package object jdbc
  extends HikariImplicits
  with ResultSetImplicits {

  type ParameterizedQuery[Self <: ParameterizedQuery[Self]] = base.ParameterizedQuery[Self, PreparedStatement, Int]

  type ParameterValue = base.ParameterValue
  val ParameterValue = base.ParameterValue

  type ParameterList = Seq[(String, Option[ParameterValue])]

  type IsParameter[T] = base.IsParameter[T, PreparedStatement, Int]

  type Index = PartialFunction[Row, Int]

  type Getter[+T] = base.Getter[Row, Index, T]

  type Connection = java.sql.Connection

  type Batchable[Key] = base.Batchable[Key, Connection, jdbc.Batch]

  type Executable[Key] = base.Executable[Key, Connection, jdbc.Execute]

  type Selectable[Key, Value] = base.Selectable[Key, Value, Connection, jdbc.Select[Value]]

  type Updatable[Key] = base.Updatable[Key, Connection, jdbc.Update]

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

  private [jdbc] def prepare(
    queryText: String,
    parameterValues: Map[String, Option[Any]],
    parameterPositions: Map[String, Set[Int]]
  )(implicit connection: Connection,
    parameterSetter: jdbc.ParameterSetter
  ): PreparedStatement = {
    val preparedStatement = connection.prepareStatement(queryText)

    bind(preparedStatement, parameterValues, parameterPositions)

    preparedStatement
  }

  private [jdbc] def bind(
    preparedStatement: PreparedStatement,
    parameterValues: Map[String, Option[Any]],
    parameterPositions: Map[String, Set[Int]]
  )(implicit parameterSetter: jdbc.ParameterSetter
  ): Unit = {
    for ((key, maybeValue) <- parameterValues) {
      val setter: Int => Unit = {
        maybeValue match {
          case None =>
            (parameterIndex: Int) => parameterSetter.setNone(preparedStatement, parameterIndex + 1)
          case Some(value) =>
            (parameterIndex: Int) => parameterSetter.setAny(preparedStatement, parameterIndex + 1, value)
        }
      }
      val parameterIndices = parameterPositions(key)
      parameterIndices.foreach(setter)
    }
  }

}
