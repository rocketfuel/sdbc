package com.rocketfuel.sdbc.base

import org.slf4j.LoggerFactory

trait Logging {

  protected val logClass: Class[_] = getClass

  protected val logger = LoggerFactory.getLogger(logClass)

}
