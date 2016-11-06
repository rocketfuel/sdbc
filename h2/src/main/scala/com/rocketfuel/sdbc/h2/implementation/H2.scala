package com.rocketfuel.sdbc.h2.implementation

import com.rocketfuel.sdbc.base.jdbc.resultset.{DefaultGetters, SeqGetter}
import com.rocketfuel.sdbc.base.jdbc.statement.{DefaultParameters, SeqParameter}
import java.nio.file.Path
import java.sql.DriverManager
import com.rocketfuel.sdbc.base.jdbc
import com.rocketfuel.sdbc.h2

abstract class H2
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
   *
   * @param name The name of the database. A name is required if you want multiple connections or dbCloseDelay != Some(0).
   * @param dbCloseDelay The number of seconds to wait after the last connection closes before deleting the database. The default None, which means never. Some(0) means right away.
   * @tparam T
   * @return
   */
  def withMemConnection[T](name: String = "", dbCloseDelay: Option[Int] = None): (Connection => T) => T = {
    val dbCloseDelayArg = s";DB_CLOSE_DELAY=${dbCloseDelay.getOrElse(-1)}"
    val connectionString = s"jdbc:h2:mem:$name$dbCloseDelayArg"
    withConnection[T](connectionString)
  }

  def withFileConnection[T](path: Path): (Connection => T) => T = {
    withConnection("jdbc:h2:" + path.toFile.getCanonicalPath)
  }

}
