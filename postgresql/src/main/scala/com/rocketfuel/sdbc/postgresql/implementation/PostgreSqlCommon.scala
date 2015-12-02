package com.rocketfuel.sdbc.postgresql.implementation

import java.io.{InputStream, Reader}
import java.sql.PreparedStatement
import java.util.UUID
import com.rocketfuel.sdbc.base.CISet
import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.postgresql.{LTree, Cidr, jdbc}
import org.json4s.JValue
import org.postgresql.PGConnection
import org.postgresql.util.PGobject
import scodec.bits.ByteVector

import scala.xml.Node

private[sdbc] abstract class PostgreSqlCommon
  extends DBMS
  with Setters
  with IntervalImplicits
  with ConnectionImplicits
  with Getters
  with Updaters {

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
   * @param connection
   */
  override def initializeConnection(connection: java.sql.Connection): Unit = {
    val pgConnection = connection.unwrap[PGConnection](classOf[PGConnection])
    pgConnection.addDataType("ltree", classOf[LTree])
    pgConnection.addDataType("inet", classOf[PGInetAddress])
    pgConnection.addDataType("cidr", classOf[Cidr])
    pgConnection.addDataType("json", classOf[PGJson])
    pgConnection.addDataType("jsonb", classOf[PGJson])
    pgConnection.addDataType("time", classOf[PGLocalTime])
  }

  override def toParameter(a: Any): Option[Any] = {
    a match {
      case null | None =>
        None
      case Some(a) =>
        Some(toParameter(a)).flatten
      case _ =>
        Some(toPostgresqlParameter(a))
    }
  }
}
