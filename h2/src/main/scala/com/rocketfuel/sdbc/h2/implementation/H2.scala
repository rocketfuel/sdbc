package com.rocketfuel.sdbc.h2.implementation

import com.rocketfuel.sdbc.base.jdbc.resultset.{DefaultGetters, SeqGetter}
import com.rocketfuel.sdbc.base.jdbc.statement.{DefaultParameters, SeqParameter}
import java.nio.file.Path
import java.sql.DriverManager
import com.rocketfuel.sdbc.base.CISet
import com.rocketfuel.sdbc.base.jdbc
import com.rocketfuel.sdbc.h2

private[sdbc] abstract class H2
  extends jdbc.DBMS
  with DefaultGetters
  with DefaultParameters
  with jdbc.DefaultUpdaters
  with SeqParameter
  with SeqGetter
  with ArrayTypes
  with SerializedParameter
  with jdbc.JdbcConnection {

  type Serialized = h2.Serialized
  val Serialized = h2.Serialized

  /**
   * Class name for the DataSource class.
   */
  override def dataSourceClassName: String = "org.h2.jdbcx.JdbcDataSource"

  /**
   * Class name for the JDBC driver class.
   */
  override def driverClassName: String = "org.h2.Driver"

  //http://www.h2database.com/html/cheatSheet.html
  override def jdbcSchemes: Set[String] = {
    CISet(
      "h2",
      "h2:mem",
      "h2:tcp"
    )
  }

  /**
   * If the JDBC driver supports the .isValid() method.
   */
  override def supportsIsValid: Boolean = true

  /**
   * The result of getMetaData.getDatabaseProductName
   */
  override def productName: String = "H2"

  /**
   *
   * @param name The name of the database. A name is required if you want multiple connections or dbCloseDelay != Some(0).
   * @param dbCloseDelay The number of seconds to wait after the last connection closes before deleting the database. The default None, which means never. Some(0) means right away.
   * @param f
   * @tparam T
   * @return
   */
  def withMemConnection[T](name: String = "", dbCloseDelay: Option[Int] = None)(f: Connection => T): T = {
    val dbCloseDelayArg = s";DB_CLOSE_DELAY=${dbCloseDelay.getOrElse(-1)}"
    val connectionString = s"jdbc:h2:mem:$name$dbCloseDelayArg"
    val connection = new Connection(DriverManager.getConnection(connectionString))
    try {
      f(connection)
    } finally {
      connection.close()
    }
  }

  def withFileConnection[T](path: Path)(f: Connection => T): T = {
    val connection = new Connection(DriverManager.getConnection("jdbc:h2:" + path.toFile.getCanonicalPath))

    try {
      f(connection)
    } finally {
      connection.close()
    }
  }

}
