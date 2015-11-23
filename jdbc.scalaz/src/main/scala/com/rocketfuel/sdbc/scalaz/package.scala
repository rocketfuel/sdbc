package com.rocketfuel.sdbc

import _root_.scalaz.stream.Process

/**
  * import this package to add a jdbc value to the standard
  * Process object.
  */
package object scalaz {

  object HasJdbc {
    val jdbc = new jdbc {}
  }

  implicit def ProcessToJdbcProcess(x: Process.type): HasJdbc.type = {
    HasJdbc
  }
}
