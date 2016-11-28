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
  self =>

  private val _keyspaces: mutable.Buffer[String] =
    mutable.Buffer.empty[String]

  def keyspaces: Vector[String] =
    _keyspaces.toVector

  def keyspace: String =
    _keyspaces.head

  override protected def beforeAll(): Unit = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("another-cassandra.yaml")
    createRandomKeyspace()
  }

  override protected def afterAll(): Unit = {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
  }

  override protected def afterEach(): Unit = {
    implicit val session = client.connect()
    try for (keyspace <- keyspaces)
      util.Try(drop(tableName = "tbl"))
    finally session.close()
  }

  override type FixtureParam = Session

  implicit val client = core.Cluster.builder().addContactPoint("localhost").withPort(9142).build()

  override protected def withFixture(test: OneArgTest): Outcome = {
    val session = client.connect()

    try test.toNoArgTest(session)()
    finally session.close()
  }

  def createRandomKeyspace(): String = {
    val keyspace = new String(util.Random.alphanumeric.filter(_.isLetter).take(10).toArray)
    implicit val session = client.connect()
    Query.execute(s"CREATE KEYSPACE $keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.close()
    _keyspaces += keyspace
    keyspace
  }

  def truncate(
    keyspace: String = self.keyspace,
    tableName: String
  )(implicit session: Session
  ): Unit = {
    Query.execute(s"TRUNCATE $keyspace.$tableName")
  }

  def drop(
    keyspace: String = self.keyspace,
    tableName: String
  )(implicit session: Session
  ): Unit = {
    Query.execute(s"DROP TABLE $keyspace.$tableName")
  }

}
