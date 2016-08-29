package com.rocketfuel.sdbc.cassandra

import com.rocketfuel.sdbc.Cassandra._

class GenericGettersSpec
  extends CassandraSuite {

  test("(Int, Int, Int)") {implicit connection =>
    Query(s"CREATE TABLE $keyspace.tbl (a int PRIMARY KEY, b int, c int)").execute()
    Query(s"INSERT INTO $keyspace.tbl (a, b, c) VALUES (1, 2, 3)").execute()

    val query = Query[(Int, Int, Int)](s"SELECT a, b, c FROM $keyspace.tbl")
    val result = query.option()
    val expected = Some((1, 2, 3))
    assertResult(expected)(result)
  }

  test("(Int, String)") {implicit connection =>
    Query(s"CREATE TABLE $keyspace.tbl (a int PRIMARY KEY, b text)").execute()
    Query(s"INSERT INTO $keyspace.tbl (a, b) VALUES (1, 'hi')").execute()

    val query = Query[(Int, String)](s"SELECT a, b FROM $keyspace.tbl")
    val result = query.option()
    val expected = Some((1, "hi"))
    assertResult(expected)(result)
  }

  test("case class TestClass(id: Int, value: String)") {implicit connection =>
    Query(s"CREATE TABLE $keyspace.tbl (a int PRIMARY KEY, b text)").execute()
    Query(s"INSERT INTO $keyspace.tbl (a, b) VALUES (1, 'hi')").execute()

    case class TestClass(id: Int, value: String)
    val query = Query[TestClass](s"SELECT a, b FROM $keyspace.tbl")
    val result = query.option()
    val expected = Some(TestClass(1, "hi"))
    assertResult(expected)(result)
  }

}
