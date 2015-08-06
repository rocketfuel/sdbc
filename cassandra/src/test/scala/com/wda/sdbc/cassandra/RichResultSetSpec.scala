package com.wda.sdbc.cassandra

import com.wda.sdbc.Cassandra._

class RichResultSetSpec
  extends CassandraSuite {

  test("iterator() works on several results") {implicit connection =>
    Select[Unit]("CREATE KEYSPACE spc WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}").execute()

    val randoms = Seq.fill(10)(util.Random.nextInt())
    Select[Int]("CREATE TABLE spc.tbl (x int PRIMARY KEY)").execute()

    val insert = Select[Int]("INSERT INTO spc.tbl (x) VALUES ($x)")

    for (random <- randoms) {
      insert.on("x" -> random).execute()
    }

    val results = Select[Int]("SELECT x FROM spc.tbl").iterator()

    assertResult(randoms.toSet)(results.toSet)
  }

  test("Insert and select works for tuples.") {implicit connection =>
    Select[Unit]("CREATE KEYSPACE spc WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}").execute()
    val randoms = Seq.fill(10)(Some(util.Random.nextInt()))
    val randoms2 = Seq.fill(10)(Some(util.Random.nextInt()))
    val tuples = randoms.zip(randoms2)
    Select[Int]("CREATE TABLE spc.tbl (x tuple<int, int> PRIMARY KEY)").execute()

    //Note: Peng verified that values in tuples are nullable, so we need
    //to support that.

    val insert = Select[Int]("INSERT INTO spc.tbl (x) VALUES ($x)")

    for (tuple <- tuples) {
      insert.on("x" -> tuple).execute()
    }

    val results = Select[(Option[Int], Option[Int])]("SELECT x FROM spc.tbl").iterator()

    assertResult(tuples.toSet)(results.toSet)
  }

  def genTuple: (Option[Int], Option[Int]) = {
    val v0Null = util.Random.nextBoolean()
    val v1Null = util.Random.nextBoolean()

    def aux(b: Boolean) = if (b) Some(util.Random.nextInt()) else None

    (aux(v0Null), aux(v1Null))
  }

  test("Insert and select works for tuples having some null elements.") {implicit connection =>
    Select[Unit]("CREATE KEYSPACE spc WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}").execute()
    val tuples = Seq.fill(10)(genTuple)
    Select[Int]("CREATE TABLE spc.tbl (x tuple<int, int> PRIMARY KEY)").execute()

    val insert = Select[Int]("INSERT INTO spc.tbl (x) VALUES ($x)")

    for (tuple <- tuples) {
      insert.on("x" -> tuple).execute()
    }

    val results = Select[(Option[Int], Option[Int])]("SELECT x FROM spc.tbl").iterator()

    assertResult(tuples.toSet)(results.toSet)
  }

}