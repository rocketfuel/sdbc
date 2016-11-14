package com.rocketfuel.sdbc.base

import org.slf4j.LoggerFactory

trait Logger {

  protected val logClass: Class[_] = getClass

  protected val log = LoggerFactory.getLogger(logClass)

}
