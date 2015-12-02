package com.rocketfuel.sdbc.sqlserver

import java.nio.ByteBuffer
import java.sql.{Date, Time, Timestamp}
import java.time._
import java.util.UUID

import com.rocketfuel.sdbc.SqlServer
import com.rocketfuel.sdbc.SqlServer._

import scalaz.Scalaz._

/**
 * Note that some of these tests can fail if Sql Server's time isn't in sync with the client running the tests.
 */
class GetterSpec
  extends SqlServerSuite {

  val uuid = UUID.randomUUID()

  testSelect[Int]("SELECT NULL", none[Int])

  testSelect[Byte]("SELECT CAST(1 AS tinyint)", 1.toByte.some)

  testSelect[Short]("SELECT CAST(1 AS smallint)", 1.toShort.some)

  testSelect[Int]("SELECT CAST(1 AS int)", 1.some)

  testSelect[Long]("SELECT CAST(1 AS bigint)", 1L.some)

  testSelect[String]("SELECT 'hello'", "hello".some)

  testSelect[ByteBuffer]("SELECT 0x0001ffa0", ByteBuffer.wrap(Array(0, 1, -1, -96).map(_.toByte)).some)

  testSelect[Float]("SELECT CAST(3.14159 AS real)", 3.14159F.some)

  testSelect[Double]("SELECT CAST(3.14159 AS float)", 3.14159.some)

  testSelect[Boolean]("SELECT CAST(1 AS bit)", true.some)

  testSelect[BigDecimal]("SELECT CAST(3.14159 AS numeric(10,5)) --as Scala BigDecimal", BigDecimal("3.14159").some)

  testSelect[java.math.BigDecimal]("SELECT CAST(3.14159 AS numeric(10,5)) --as Java BigDecimal", BigDecimal("3.14159").underlying.some)

  testSelect[scala.xml.Node]("SELECT CAST('<a>hi</a>' AS xml)", <a>hi</a>.some)

  testSelect[Date]("SELECT CAST('2014-12-29' AS date)", Date.valueOf("2014-12-29").some)

  testSelect[Time]("SELECT CAST('03:04:05' AS time) --as JDBC Time", Time.valueOf("03:04:05").some)

  testSelect[LocalTime]("SELECT CAST('03:04:05' AS time) --as Java 8 LocalTime", LocalTime.parse("03:04:05").some)

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

  testSelect[OffsetDateTime]("SELECT CAST('2014-12-29 01:02:03.5 -4:00' AS datetimeoffset) --as Java 8 OffsetDateTime", OffsetDateTime.parse("2014-12-29T01:02:03.5-04:00").some)

  testSelect[Instant]("SELECT CAST('2014-12-29 01:02:03.5 -4:00' AS datetimeoffset) --as Java 8 Instant", Instant.parse("2014-12-29T05:02:03.5Z").some)

  testSelect[HierarchyId]("SELECT CAST('/' AS hierarchyid).ToString()", HierarchyId.empty.some)

  testSelect[HierarchyId]("SELECT CAST('/1/2/3/' AS hierarchyid).ToString()", HierarchyId(1, 2, 3).some)

  testSelect[HierarchyId]("SELECT CAST('/1/2.1/3/' AS hierarchyid).ToString()", HierarchyId(1, Seq(2, 1), 3).some)

  testSelect[UUID](s"SELECT CAST('$uuid' AS uniqueidentifier)", uuid.some)

}
