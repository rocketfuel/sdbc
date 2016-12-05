package com.rocketfuel.sdbc.mariadb

import com.rocketfuel.sdbc.MariaDb._

class MultiQuerySpec
  extends MariaDbSuite {

  test("vector vector") {implicit connection =>
    val tbl0 = util.Random.nextString(10)
    val tbl1 = util.Random.nextString(10)
    Ignore.ignore(s"CREATE TABLE `$tbl0` (x int); CREATE TABLE `$tbl1` (x int); INSERT INTO `$tbl0` (x) VALUES (1); INSERT INTO `$tbl1` (x) VALUES(2);")

    val (results0, results1) =
      MultiQuery.result[(QueryResult.Vector[Int], QueryResult.Vector[Int])](s"SELECT * FROM `$tbl0`; SELECT * FROM `$tbl1`")

    assertResult(Vector(1))(results0.get)
    assertResult(Vector(2))(results1.get)
  }

  test("update vector") {implicit connection =>
    val tbl = util.Random.nextString(10)
    Ignore.ignore(s"CREATE TABLE `$tbl` (i int PRIMARY KEY)")

    val (results0, results1) =
      MultiQuery.result[(QueryResult.Update, QueryResult.Vector[Int])](s"INSERT INTO `$tbl`(i) VALUES (1); SELECT i FROM `$tbl`;")

    assertResult(1)(results0.get)
    assertResult(Vector(1))(results1.get)
  }

  test("iterator vector") {implicit connection =>
    val tbl = util.Random.nextString(10)
    Ignore.ignore(s"CREATE TABLE `$tbl` (i int PRIMARY KEY)")
    Ignore.ignore(s"INSERT INTO `$tbl`(i) VALUES (1)")

    val (results0, results1) =
      MultiQuery.result[(QueryResult.Iterator[Int], QueryResult.Vector[Int])](s"SELECT i FROM `$tbl`; SELECT i FROM `$tbl`;")

    assertResult(Vector(1))(results0.get.toVector)
    assertResult(Vector(1))(results1.get)
  }

  test("iterator singleton") {implicit connection =>
    val tbl = util.Random.nextString(10)
    Ignore.ignore(s"CREATE TABLE `$tbl` (i int PRIMARY KEY)")
    Ignore.ignore(s"INSERT INTO `$tbl`(i) VALUES (1)")

    val (results0, results1) =
      MultiQuery.result[(QueryResult.Iterator[Int], QueryResult.Singleton[Int])](s"SELECT i FROM `$tbl`; SELECT i FROM `$tbl`;")

    assertResult(Vector(1))(results0.get.toVector)
    assertResult(1)(results1.get)
  }

  test("iterator option") {implicit connection =>
    val tbl = util.Random.nextString(10)
    Ignore.ignore(s"CREATE TABLE `$tbl` (i int PRIMARY KEY)")
    Ignore.ignore(s"INSERT INTO `$tbl`(i) VALUES (1)")

    val (results0, results1) =
      MultiQuery.result[(QueryResult.Iterator[Int], QueryResult.Option[Int])](s"SELECT i FROM `$tbl`; SELECT i FROM `$tbl`;")

    assertResult(Vector(1))(results0.get.toVector)
    assertResult(Some(1))(results1.get)
  }

  test("iterator Some(None)") {implicit connection =>
    val tbl = util.Random.nextString(10)
    Ignore.ignore(s"CREATE TABLE `$tbl` (i int PRIMARY KEY auto_increment, v int NULL)")
    Ignore.ignore(s"INSERT INTO `$tbl`(v) VALUES (NULL)")

    val (results0, results1) =
      MultiQuery.result[(QueryResult.Iterator[Option[Int]], QueryResult.Option[Option[Int]])](s"SELECT v FROM `$tbl`; SELECT v FROM `$tbl`;")

    assertResult(Vector[Option[Int]](None))(results0.get.toVector)

    /*
    This deserves some explanation. The outer Option being Some means
    that there was a row. The inner Option being None means that
    the value in the row was NULL.
     */
    assertResult(Some(None))(results1.get)
  }

}
