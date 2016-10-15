package com.rocketfuel.sdbc.base.jdbc

import java.sql.{DriverPropertyInfo, Driver}
import java.util.Properties
import java.util.logging.Logger

class TestDriver extends Driver {
  override def acceptsURL(url: String): Boolean = ???

  override def jdbcCompliant(): Boolean = ???

  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] = ???

  override def getMinorVersion: Int = ???

  override def getParentLogger: Logger = ???

  override def connect(url: String, info: Properties): java.sql.Connection = ???

  override def getMajorVersion: Int = ???
}
