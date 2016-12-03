package com.rocketfuel.sdbc.base

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

trait Logger {

  protected def logClass: Class[_] = getClass

  protected val log = Logger(LoggerFactory.getLogger(logClass))

}
