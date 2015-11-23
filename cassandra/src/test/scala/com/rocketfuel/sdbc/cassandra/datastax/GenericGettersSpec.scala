package com.rocketfuel.sdbc.cassandra.datastax

class GenericGettersSpec
  extends DatastaxSuite {

  test("(Int, Int, Int)") {implicit connection =>
    Execute("CREATE KEYSPACE spc WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}").execute()
    Execute("CREATE TABLE spc.tbl (a int PRIMARY KEY, b int, c int)").execute()
    Execute("INSERT INTO spc.tbl (a, b, c) VALUES (1, 2, 3)").execute()

    val query = Select[(Int, Int, Int)]("SELECT a, b, c FROM spc.tbl")
    val result = query.option()
    val expected = Some((1, 2, 3))
    assertResult(expected)(result)
  }

  test("(Int, String)") {implicit connection =>
    Execute("CREATE KEYSPACE spc WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}").execute()
    Execute("CREATE TABLE spc.tbl (a int PRIMARY KEY, b text)").execute()
    Execute("INSERT INTO spc.tbl (a, b) VALUES (1, 'hi')").execute()

    val query = Select[(Int, String)]("SELECT a, b FROM spc.tbl")
    val result = query.option()
    val expected = Some((1, "hi"))
    assertResult(expected)(result)
  }

  test("case class TestClass(id: Int, value: String)") {implicit connection =>
    Execute("CREATE KEYSPACE spc WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}").execute()
    Execute("CREATE TABLE spc.tbl (a int PRIMARY KEY, b text)").execute()
    Execute("INSERT INTO spc.tbl (a, b) VALUES (1, 'hi')").execute()

    case class TestClass(id: Int, value: String)
    val query = Select[TestClass]("SELECT a, b FROM spc.tbl")
    val result = query.option()
    val expected = Some(TestClass(1, "hi"))
    assertResult(expected)(result)
  }

}
