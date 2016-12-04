package com.rocketfuel.sdbc.mariadb

import com.rocketfuel.sdbc.MariaDb._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class HasMariaDbPoolSpec
  extends FunSuite
  with HasMariaDbPool
  with BeforeAndAfterAll {

  def testDatabaseExists(): Boolean = {
    withMaria[Boolean] { implicit connection =>
      Select[Boolean]("SELECT TRUE").singleton()
    }
  }

  test("creates and destroys test database") {
    mariaStart()

    assert(testDatabaseExists())
  }

  test("connection unwrap works") {
    val connection = mariaPool.get.getConnection()
    try assert(connection.getUsername != null)
    finally connection.close()
  }

  override protected def afterAll(): Unit = mariaStop()

}
