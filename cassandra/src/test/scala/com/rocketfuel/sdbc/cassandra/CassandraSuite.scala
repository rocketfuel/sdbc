package com.rocketfuel.sdbc.cassandra

import com.rocketfuel.sdbc.Cassandra._
import com.datastax.driver.core
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome, fixture}

abstract class CassandraSuite
  extends fixture.FunSuite
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  protected var keyspace: String = null

  override protected def beforeAll(): Unit = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("another-cassandra.yaml")
  }

  override protected def beforeEach(): Unit = {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    implicit val session = client.connect()
    keyspace = randomKeyspace()
    session.close()
  }

  override type FixtureParam = Session

  implicit val client = core.Cluster.builder().addContactPoint("localhost").withPort(9142).build()

  override protected def withFixture(test: OneArgTest): Outcome = {
    val session = client.connect()
    try {
      test.toNoArgTest(session)()
    } finally {
      session.close()
    }
  }

  def randomKeyspace()(implicit session: Session): String = {
    val space = new String(util.Random.alphanumeric.filter(_.isLetter).take(10).toArray)
    Query(s"CREATE KEYSPACE $space WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}").io.execute()
    space
  }

  def truncate()(implicit session: Session): Unit = {
    Query(s"TRUNCATE $keyspace.tbl").io.execute()
  }

}
