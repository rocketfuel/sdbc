package com.rocketfuel.sdbc.base.jdbc

import java.io.PrintWriter
import java.util.logging.Logger
import javax.sql.DataSource

class TestDataSource extends DataSource {
  override def getConnection: java.sql.Connection = ???

  override def getConnection(username: String, password: String): java.sql.Connection = ???

  override def setLogWriter(out: PrintWriter): Unit = ???

  override def getLoginTimeout: Int = ???

  override def setLoginTimeout(seconds: Int): Unit = ???

  override def getParentLogger: Logger = ???

  override def getLogWriter: PrintWriter = ???

  override def unwrap[T](iface: Class[T]): T = ???

  override def isWrapperFor(iface: Class[_]): Boolean = ???
}
