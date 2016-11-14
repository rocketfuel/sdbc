package com.rocketfuel.sdbc.cassandra

import com.rocketfuel.sdbc.Cassandra._
import com.datastax.driver.core
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome, fixture}
import scala.collection.mutable

abstract class CassandraSuite
  extends fixture.FunSuite
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  private val _keyspaces: mutable.Buffer[String] =
    mutable.Buffer.empty[String]

  def keyspaces: Vector[String] =
    _keyspaces.toVector

  def keyspace: String =
    _keyspaces.head

  override protected def beforeAll(): Unit = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("another-cassandra.yaml")
  }

  override protected def beforeEach(): Unit = {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    createRandomKeyspace()
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

  def createRandomKeyspace(): String = {
    val keyspace = new String(util.Random.alphanumeric.filter(_.isLetter).take(10).toArray)
    implicit val session = client.connect()
    Query.execute(s"CREATE KEYSPACE $keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.close()
    _keyspaces += keyspace
    keyspace
  }

  def truncate()(implicit session: Session): Unit = {
    for (keyspace <- _keyspaces)
      Query(s"TRUNCATE $keyspace.tbl").execute()
  }

}
