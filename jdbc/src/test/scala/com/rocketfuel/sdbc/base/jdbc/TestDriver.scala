package com.rocketfuel.sdbc.base.jdbc

import java.sql.{Connection, DriverPropertyInfo, Driver}
import java.util.Properties
import java.util.logging.Logger

/**
  * Created by jshaw on 12/7/15.
  */
class TestDriver extends Driver {
  override def acceptsURL(url: String): Boolean = ???

  override def jdbcCompliant(): Boolean = ???

  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] = ???

  override def getMinorVersion: Int = ???

  override def getParentLogger: Logger = ???

  override def connect(url: String, info: Properties): Connection = ???

  override def getMajorVersion: Int = ???
}
