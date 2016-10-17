package com.rocketfuel.sdbc.h2

import java.nio.ByteBuffer
import java.sql.{Date, Time, Timestamp}
import java.time._
import java.util.UUID
import scalaz.Scalaz._
import com.rocketfuel.sdbc.H2._

class GettersSpec
  extends H2Suite {

  def testAux[T](
    query: String,
    expectedValue: Option[T]
  )(implicit converter: RowConverter[Option[T]]
  ): Connection => Unit = { implicit connection =>
    val result = Select[Option[T]](query).option().flatten
    (expectedValue, result) match {
      case (Some(expectedArray: Array[_]), Some(resultArray: Array[_])) =>
        assert(expectedArray.sameElements(resultArray))
      case (expected, actual) =>
        assertResult(expected)(actual)
    }
  }

  def testSelect[T](
    query: String,
    expectedValue: Option[T]
  )(implicit converter: RowConverter[Option[T]]
  ): Unit = {
    test(query)(testAux[T](query, expectedValue))
  }

  def testIgnore[T](
    query: String,
    expectedValue: Option[T]
  )(implicit converter: RowConverter[Option[T]]
  ): Unit = {
    ignore(query)(testAux[T](query, expectedValue))
  }

  val uuid = UUID.randomUUID()

  testSelect[Int]("SELECT NULL", none[Int])

  testSelect[Byte]("SELECT CAST(1 AS tinyint)", 1.toByte.some)

  testSelect[Short]("SELECT CAST(1 AS smallint)", 1.toShort.some)

  testSelect[Int]("SELECT CAST(1 AS int)", 1.some)

  testSelect[Long]("SELECT CAST(1 AS bigint)", 1L.some)

  testSelect[String]("SELECT 'hello'", "hello".some)

  testSelect[ByteBuffer]("SELECT X'0001ffa0'", ByteBuffer.wrap(Array[Byte](0, 1, -1, -96)).some)

  testSelect[Float]("SELECT CAST(3.14159 AS real)", 3.14159F.some)

  testSelect[Double]("SELECT CAST(3.14159 AS float)", 3.14159.some)

  testSelect[Boolean]("SELECT CAST(1 AS bit)", true.some)

  testSelect[BigDecimal]("SELECT CAST(3.14159 AS numeric(10,5)) --as Scala BigDecimal", BigDecimal("3.14159").some)

  testSelect[java.math.BigDecimal]("SELECT CAST(3.14159 AS numeric(10,5)) --as Java BigDecimal", BigDecimal("3.14159").underlying.some)

  testSelect[Date]("SELECT CAST('2014-12-29' AS date)", Date.valueOf("2014-12-29").some)

  testSelect[Time]("SELECT CAST('03:04:05' AS time) --as JDBC Time", Time.valueOf("03:04:05").some)

  testSelect[Timestamp]("SELECT CAST('2014-12-29 01:02:03.5' AS datetime)", Timestamp.valueOf("2014-12-29 01:02:03.5").some)

  testSelect[LocalDateTime]("SELECT CAST('2014-12-29 01:02:03.5' AS datetime) --as Java 8 LocalDateTime)", LocalDateTime.parse("2014-12-29T01:02:03.5").some)

  {
    //Convert the time being tested into UTC time
    //using the current time zone's offset at the time
    //that we're testing.
    //We can't use the current offset, because of, for example,
    //daylight savings.
    val localTime = LocalDateTime.parse("2014-12-29T01:02:03.5")
    val offset = ZoneId.systemDefault().getRules.getOffset(localTime)
    val expectedTime = localTime.toInstant(offset)
    testSelect[Instant]("SELECT CAST('2014-12-29 01:02:03.5' AS datetime) --as Java 8 Instant", expectedTime.some)
  }

  testSelect[UUID](s"SELECT CAST('$uuid' AS uuid)", uuid.some)

  testSelect[Seq[Int]]("SELECT (1, 2, 3)", Seq(1, 2, 3).some)

  testSelect[Seq[Option[Int]]]("SELECT (1, NULL, 3)", Seq(1.some, none[Int], 3.some).some)

  testSelect[Seq[Seq[Int]]]("SELECT (())", Seq.empty.some)

  testIgnore[Seq[Seq[Int]]]("SELECT ((1, 2),)", Seq(Seq(1, 2)).some)

  testIgnore[Seq[Seq[Option[Int]]]]("SELECT ((1, NULL), (2, NULL))", Seq(Seq(Some(1), None), Seq(Some(2), None)).some)

}
