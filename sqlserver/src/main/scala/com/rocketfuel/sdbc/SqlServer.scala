package com.rocketfuel.sdbc

import com.rocketfuel.sdbc.sqlserver.implementation

/**
 * Import the contents of this package to interact with [[http://www.microsoft.com/en-us/server-cloud/products/sql-server/ Microsoft SQL Server]] using JDBC.
 *
 * {{{
 * import com.rocketfuel.sdbc.SqlServer._
 *
 * val pool = Pool(...)
 *
 * pool.withConnection(_.iterator[Int]("SELECT 1").toSeq)
 * }}}
 */
case object SqlServer extends implementation.SqlServer
