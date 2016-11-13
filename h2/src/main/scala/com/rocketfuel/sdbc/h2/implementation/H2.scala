package com.rocketfuel.sdbc.h2.implementation

import com.rocketfuel.sdbc.base.jdbc.resultset.{DefaultGetters, SeqGetter}
import com.rocketfuel.sdbc.base.jdbc.statement.{DefaultParameters, SeqParameter}
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
    * A convenience method for performing some action with an in-memory database. If you want a connection
    * to a file, construct a query string the instructions at [[http://www.h2database.com/html/features.html#database_url]]
    * and use a method in [[Connection$]].
    *
    * To connect to a remote database, it's best to use a [[Pool]], or if you want a single connection,
    * construct a query string and use one of the methods on [[Connection$]].
    *
    * Be sure to not return the connection, because it will be closed.
    * @param name The name of the database. A name is required if you want multiple connections or dbCloseDelay != Some(0).
    * @param dbCloseDelay The number of seconds to wait after the last connection closes before deleting the database. The default None, which means never. Some(0) means right away.
    * @tparam T
    * @return
    */
  def withMemConnection[T](name: String = "", dbCloseDelay: Option[Int] = None): (Connection => T) => T = {
    val dbCloseDelayArg = s";DB_CLOSE_DELAY=${dbCloseDelay.getOrElse(-1)}"
    val connectionString = s"jdbc:h2:mem:$name$dbCloseDelayArg"
    Connection.using[T](connectionString)
  }

}
