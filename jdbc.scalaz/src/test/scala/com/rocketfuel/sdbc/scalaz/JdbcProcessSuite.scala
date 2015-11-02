package com.rocketfuel.sdbc.scalaz

import com.rocketfuel.sdbc.h2.jdbc.H2._
import com.rocketfuel.sdbc.h2.jdbc.H2Suite
import com.zaxxer.hikari.HikariConfig
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import scalaz.stream._

abstract class JdbcProcessSuite
  extends H2Suite
  with BeforeAndAfterEach
  with BeforeAndAfterAll {
  suite =>

  implicit val pool = {
    val poolConfig = new HikariConfig()
    poolConfig.setJdbcUrl("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")

    Pool(poolConfig)
  }

  val expectedCount = 100L

  case class LongKey(key: Long)

  implicit val LongInsertable = new Updatable[LongKey] {
    val update = Update("INSERT INTO tbl (i) VALUES (@i)")

    override def update(key: LongKey): Update = {
      update.on("i" -> key.key)
    }
  }

  val insertSet = 0L.until(expectedCount).toSet

  val inserts = {
    Process.emitAll(insertSet.toSeq.map(key => LongKey(key)))
  }

  val select = Select[Int]("SELECT i FROM tbl")

  override protected def beforeEach(): Unit = {
    withMemConnection(name = "test", dbCloseDelay = None) { implicit connection: Connection =>
      Execute("CREATE TABLE tbl (i bigint PRIMARY KEY)").execute()
    }
  }

  override protected def afterEach(): Unit = {
    withMemConnection(name = "test", dbCloseDelay = None) { implicit connection: Connection =>
      Execute("DROP TABLE tbl").execute()
    }
  }

  override protected def afterAll(): Unit = {
    pool.close()
  }

}
