package com.rocketfuel.sdbc.h2.implementation

import java.nio.file.Path
import java.sql.DriverManager
import com.rocketfuel.sdbc.base.CISet
import com.rocketfuel.sdbc.base.jdbc

private[sdbc] abstract class H2
  extends jdbc.DBMS
  with jdbc.DefaultGetters
  with jdbc.DefaultParameters
  with jdbc.DefaultUpdaters
  with SerializedParameter
  with SeqParameter {

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
   * @param dbCloseDelay The number of seconds to wait after the last connection closes before deleting the database. The default is right away, or Some(0). None means never.
   * @param f
   * @tparam T
   * @return
   */
  def withMemConnection[T](name: String = "", dbCloseDelay: Option[Int] = Some(0))(f: Connection => T): T = {
    val dbCloseDelayArg = s";DB_CLOSE_DELAY=${dbCloseDelay.getOrElse(-1)}"
    val connectionString = s"jdbc:h2:mem:$name$dbCloseDelayArg"
    val connection = DriverManager.getConnection(connectionString)
    try {
      f(connection)
    } finally {
      connection.close()
    }
  }

  def withFileConnection[T](path: Path)(f: Connection => T): T = {
    val connection = DriverManager.getConnection("jdbc:h2:" + path.toFile.getCanonicalPath)

    try {
      f(connection)
    } finally {
      connection.close()
    }
  }

}
