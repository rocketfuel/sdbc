package com.rocketfuel.sdbc.cassandra

import com.rocketfuel.sdbc.Cassandra._
import com.datastax.oss.driver.api.core._
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader
import com.typesafe.config.{Config, ConfigFactory}
import java.net.InetSocketAddress
import java.util.function.Supplier
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome, fixture}

// tests required increasing the default timeout
object ConfigLoader extends DefaultDriverConfigLoader {
  self =>
  override def getConfigSupplier: Supplier[Config] = {
    new Supplier[Config] {
      override def get(): Config = {
        val config = ConfigLoader.super.getConfigSupplier.get()
        config.withFallback(ConfigFactory.parseString(s"${DefaultDriverOption.REQUEST_TIMEOUT.getPath}: 10 seconds"))
      }
    }
  }
}

abstract class CassandraSuite
  extends fixture.FunSuite
  with BeforeAndAfterAll
  with BeforeAndAfterEach {
  self =>

  val keyspace = "k"

  override protected def beforeAll(): Unit = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra()
    createKeyspace()
  }

  override protected def afterAll(): Unit = {
    dropKeyspace()
  }

  def getSession(keyspace: String = keyspace): CqlSession = {
    CqlSession.builder
      .addContactPoint(new InetSocketAddress(EmbeddedCassandraServerHelper.getHost, EmbeddedCassandraServerHelper.getNativeTransportPort))
      .withConfigLoader(ConfigLoader)
      .withLocalDatacenter("datacenter1")
      .withKeyspace(keyspace)
      .build
  }

  override protected def afterEach(): Unit = {
    implicit val session = getSession()
    util.Try(drop(tableName = "tbl")).failed.foreach(_.printStackTrace())
    session.close()
  }

  override type FixtureParam = Session

  override protected def withFixture(test: OneArgTest): Outcome = {
    val session = getSession()

    try test.toNoArgTest(session)()
    finally session.close()
  }

  def createKeyspace(): Unit = synchronized {
    implicit val session = getSession(null)
    try Query.execute(s"CREATE KEYSPACE $keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    finally session.close()
  }

  def dropKeyspace(): Unit = synchronized {
    implicit val session = getSession(null)
    try Query.execute(s"DROP KEYSPACE $keyspace")
    finally session.close()
  }

  def truncate(
    tableName: String
  )(implicit session: Session
  ): Unit = {
    Query.execute(s"TRUNCATE $keyspace.$tableName")
  }

  def drop(
    tableName: String
  )(implicit session: Session
  ): Unit = {
    Query.execute(s"DROP TABLE $keyspace.$tableName")
  }

}
