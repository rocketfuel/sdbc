package com.rocketfuel.sdbc.mariadb

import com.rocketfuel.sdbc.MariaDb._
import java.nio.ByteBuffer
import java.sql.{Date, Time, Timestamp}
import java.time._
import scalaz.Scalaz._

class GettersSpec
  extends MariaDbSuite {

  def testAux[T](
    sqlType: String,
    sqlValue: String,
    expectedValue: Option[T]
  )(implicit converter: RowConverter[Option[T]]
  ): Connection => Unit = { implicit connection =>
    Ignore.ignore(s"CREATE TABLE tbl (id int primary key auto_increment, x $sqlType NULL)")
    try {
      assertResult(1)(Update(s"INSERT INTO tbl (x) VALUES ($sqlValue)").update())
      val result = Select[Option[T]]("SELECT x FROM tbl").one()
      assertResult(expectedValue)(result)
    } finally util.Try(Ignore.ignore(s"DROP TABLE tbl"))
  }

  def testSelect[T](
    sqlType: String,
    sqlValue: String,
    expectedValue: Option[T]
  )(implicit converter: RowConverter[Option[T]]
  ): Unit = {
    test(s"select $sqlValue as $sqlType")(testAux[T](sqlType, sqlValue, expectedValue))
  }

  testSelect[Int]("int", "NULL", none[Int])

  testSelect[Byte]("tinyint", "1", 1.toByte.some)

  testSelect[Short]("smallint", "1", 1.toShort.some)

  testSelect[Int]("int", "1", 1.some)

  testSelect[Long]("bigint", "1", 1L.some)

  testSelect[String]("tinytext", "'hello'", "hello".some)

  testSelect[ByteBuffer]("TINYBLOB", "X'0001ffa0'", ByteBuffer.wrap(Array[Byte](0, 1, -1, -96)).some)

  testSelect[Float]("float", "3.14159", 3.14159F.some)

  testSelect[Double]("double", "3.14159", 3.14159.some)

  testSelect[Boolean]("boolean", "TRUE", true.some)

  testSelect[BigDecimal]("decimal(10,5)", "3.14159", BigDecimal("3.14159").some)

  testSelect[java.math.BigDecimal]("decimal(10,5)", "3.14158", BigDecimal("3.14158").underlying.some)

  testSelect[Date]("date", "'2014-12-29'", Date.valueOf("2014-12-29").some)

  testSelect[Time]("time", "'03:04:05'", Time.valueOf("03:04:05").some)

  testSelect[Timestamp]("timestamp(1)", "'2014-12-29 01:02:03.5'", Timestamp.valueOf("2014-12-29 01:02:03.5").some)

  testSelect[Timestamp]("datetime(1)", "'2014-12-29 01:02:03.6'", Timestamp.valueOf("2014-12-29 01:02:03.6").some)

  {
    //Convert the time being tested into UTC time
    //using the current time zone's offset at the time
    //that we're testing.
    //We can't use the current offset, because of, for example,
    //daylight savings.
    val localTime = LocalDateTime.parse("2014-12-29T01:02:03.7")
    val offset = ZoneId.systemDefault().getRules.getOffset(localTime)
    val expectedTime = localTime.toInstant(offset)
    testSelect[Instant]("datetime(1)", "'2014-12-29 01:02:03.7'", expectedTime.some)
  }

}
