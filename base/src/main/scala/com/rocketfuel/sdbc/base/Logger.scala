package com.rocketfuel.sdbc.base

import org.slf4j.LoggerFactory

trait Logger {

  protected def logClass: Class[_] = getClass

  protected val log = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(logClass))

}
