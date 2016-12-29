package com.rocketfuel.sdbc.cassandra

import com.rocketfuel.sdbc.Cassandra._
import com.rocketfuel.sdbc.Cassandra.syntax._
import org.scalatest.FunSuite

class syntaxSpec extends FunSuite {

  test("syntax works") {
    implicit def x: Queryable[Int, Int] = ???
    implicit def session: Session = ???

    assertCompiles("""3.iterator()""")
  }

}
