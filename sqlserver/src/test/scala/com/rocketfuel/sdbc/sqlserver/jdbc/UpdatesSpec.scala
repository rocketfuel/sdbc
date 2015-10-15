package com.rocketfuel.sdbc.sqlserver.jdbc

import java.sql.{Time, Date, Timestamp}
import java.time.{LocalTime, LocalDate, Instant}
import java.util.UUID

import com.rocketfuel.sdbc.base.jdbc.Updater
import scodec.bits.ByteVector

import scala.reflect.ClassTag

class UpdatesSpec extends SqlServerSuite {

  def testUpdate[T](
    typeName: String
  )(before: T
  )(after: T
  )(implicit ctag: ClassTag[T],
    updater: Updater[T],
    converter: CompositeGetter[T]
  ): Unit = {
    test(s"Update ${ctag.runtimeClass.getName}") {implicit connection =>
      Update(s"CREATE TABLE tbl (id int identity PRIMARY KEY, v $typeName)").update()

      update"INSERT INTO tbl (v) VALUES ($before)".update()

      for (row <- selectForUpdate"SELECT * FROM tbl".iterator()) {
        row("v") = after
        row.updateRow()
      }

      val maybeValue = Select[T]("SELECT v FROM tbl").option()

      assert(maybeValue.nonEmpty)

      assertResult(Some(after))(maybeValue)
    }
  }

  testUpdate[Long]("bigint")(1L)(2L)

  testUpdate[Int]("int")(1)(2)

  testUpdate[Short]("smallint")(1.toShort)(2.toShort)

  testUpdate[Byte]("tinyint")(1.toByte)(2.toByte)

  testUpdate[Double]("float")(1.0)(2.0)

  testUpdate[Float]("real")(1.0F)(2.0F)

  testUpdate[java.lang.Long]("bigint")(1L)(2L)

  testUpdate[java.lang.Integer]("int")(1)(2)

  testUpdate[java.lang.Short]("smallint")(1.toShort)(2.toShort)

  testUpdate[java.lang.Byte]("tinyint")(1.toByte)(2.toByte)

  testUpdate[java.lang.Double]("float")(1.0)(2.0)

  testUpdate[java.lang.Float]("real")(1.0F)(2.0F)

  testUpdate[ByteVector]("varbinary(max)")(ByteVector(1, 2, 3))(ByteVector(4, 5, 6))

  testUpdate[BigDecimal]("decimal")(BigDecimal(3))(BigDecimal("500"))

  testUpdate[Timestamp]("datetime2")(new Timestamp(0))(Timestamp.from(Instant.now()))

  testUpdate[Date]("date")(new Date(0))(Date.valueOf(LocalDate.now()))

  testUpdate[Time]("time")(new Time(0))(Time.valueOf(LocalTime.now()))

  testUpdate[Instant]("datetime2")(Instant.ofEpochMilli(0))(Instant.now())

  testUpdate[LocalDate]("date")(LocalDate.ofEpochDay(0))(LocalDate.now())

  testUpdate[LocalTime]("time")(LocalTime.of(0, 0, 0))(LocalTime.now())

  testUpdate[Boolean]("bit")(false)(true)

  testUpdate[String]("varchar(max)")("hi")("bye")

  testUpdate[UUID]("uniqueidentifier")(UUID.randomUUID())(UUID.randomUUID())

  test(s"Update HierarchyId") {implicit connection =>
    val before = HierarchyId()
    val after = HierarchyId(1, 2)

    Update(s"CREATE TABLE tbl (id int identity PRIMARY KEY, v hierarchyid)").update()

    update"INSERT INTO tbl (v) VALUES ($before)".update()

    for (row <- selectForUpdate"SELECT id, v FROM tbl".iterator()) {
      row("v") = after
      row.updateRow()
    }

    val maybeValue = Select[HierarchyId]("SELECT v.ToString() FROM tbl").option()

    assert(maybeValue.nonEmpty)

    assertResult(Some(after))(maybeValue)
  }

  test(s"Update None") {implicit connection =>
    val before = Some(1)
    val after = None

    Update(s"CREATE TABLE tbl (id int identity PRIMARY KEY, v int)").update()

    update"INSERT INTO tbl (v) VALUES ($before)".update()

    for (row <- selectForUpdate"SELECT id, v FROM tbl".iterator()) {
      row("v") = after
      row.updateRow()
    }

    val maybeRow = Select[Option[Int]]("SELECT v FROM tbl").iterator.toStream.headOption

    assert(maybeRow.nonEmpty, "There was a row")

    val maybeValue = maybeRow.get

    assert(maybeValue.isEmpty)
  }

}
