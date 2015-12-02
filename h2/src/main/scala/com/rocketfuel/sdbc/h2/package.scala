package com.rocketfuel.sdbc

/**
  * Import the contents of this package to interact with [[http://www.h2database.com/html/main.html H2]] using JDBC.
  *
  * {{{
  * import com.rocketfuel.sdbc.h2.jdbc.H2._
  *
  * val pool = Pool(...)
  *
  * pool.withConnection(_.iterator[Int]("SELECT 1").toSeq)
  * }}}
  */
package object h2 extends implementation.H2 {

}
