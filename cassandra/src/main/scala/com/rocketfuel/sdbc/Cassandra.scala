package com.rocketfuel.sdbc

/**
  * Import the contents of this object to interact with [[http://cassandra.apache.org/ Apache Cassandra]] using the Datastax driver.
  *
  * {{{
  * import com.rocketfuel.sdbc.Cassandra._
  *
  * val session = cluster.connect()
  *
  * session.iterator[Int]("SELECT 1")
  * }}}
  */
object Cassandra extends cassandra.implementation.Cassandra {

}
