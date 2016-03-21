package com.rocketfuel.sdbc.postgresql.implementation

import com.rocketfuel.sdbc.base.CISet
import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.base.jdbc.statement.{SeqParameter, XmlParameter}
import com.rocketfuel.sdbc.postgresql.{Cidr, LTree}
import java.sql.SQLException
import org.postgresql.PGConnection
import org.postgresql.core.BaseConnection

private[sdbc] abstract class PostgreSql
  extends DBMS
  with Setters
  with IntervalImplicits
  with Getters
  with Updaters
  with XmlParameter
  with SeqParameter
  with ArrayTypes {

  override def dataSourceClassName = "org.postgresql.ds.PGSimpleDataSource"
  override def driverClassName = "org.postgresql.Driver"
  override def jdbcSchemes = CISet("postgresql")
  override def productName: String = "PostgreSQL"
  override val supportsIsValid: Boolean = true

  /**
   * Perform any connection initialization that should be done when a connection
   * is created. EG add a type mapping.
   *
   * By default this method does nothing.
 *
   * @param connection
   */
  override def initializeConnection(connection: java.sql.Connection): Unit = {
    val pgConnection = connection.unwrap[PGConnection](classOf[PGConnection])
    pgConnection.addDataType("ltree", classOf[LTree])
    pgConnection.addDataType("inet", classOf[PGInetAddress])
    pgConnection.addDataType("cidr", classOf[Cidr])
    pgConnection.addDataType("json", classOf[PGJson])
    pgConnection.addDataType("jsonb", classOf[PGJson])
    //The PG driver won't use these registered custom classes.
//    pgConnection.addDataType("time", classOf[PGLocalTime])
//    pgConnection.addDataType("timetz", classOf[PGTimeTz])
//    pgConnection.addDataType("timestamptz", classOf[PGTimestampTz])
  }

  /** This can be used to get to the getCopyApi() and other methods.
 *
    * @param connection The Connection or Hikari Connection which contains an underlying PGConnection.
    * @return The underlying PGConnection.
    * @throws SQLException if the connection is not a PGConnection.
    */
  implicit def PostgreSqlConnectionToPGConnection(connection: Connection): BaseConnection = {
    connection.unwrap(classOf[BaseConnection])
  }

}
