package com.rocketfuel.sdbc.sqlserver

import java.sql.{Date, Time, Timestamp}
import java.time.{Instant, LocalDate, LocalTime}
import java.util.UUID
import scodec.bits.ByteVector
import com.rocketfuel.sdbc.SqlServer._

import scala.reflect.ClassTag

class UpdatesSpec extends SqlServerSuite {

  def testUpdate[T](
    typeName: String
  )(before: T
  )(after: T
  )(implicit ctag: ClassTag[T],
    updater: Updater[T],
    setter: T => ParameterValue,
    converter: StatementConverter[Option[T]]
  ): Unit = {
    test(s"Update ${ctag.runtimeClass.getName}") {implicit connection =>
      Select[UpdateCount](s"CREATE TABLE tbl (id int identity PRIMARY KEY, v $typeName)").run()

      Select[UpdateCount]("INSERT INTO tbl (v) VALUES (@before)").on("before" -> before).run()

      for (row <- selectForUpdate"SELECT * FROM tbl".run()) {
        row("v") = after
        row.updateRow()
      }

      val maybeValue = Select[Option[T]]("SELECT v FROM tbl").run()

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

  testUpdate[Date]("date")(new Date(0))(Date.valueOf(LocalDate.now()))

  testUpdate[Time]("time")(new Time(0))(Time.valueOf(LocalTime.now()))

  testUpdate[LocalDate]("date")(LocalDate.ofEpochDay(0))(LocalDate.now())

  testUpdate[LocalTime]("time")(LocalTime.of(0, 0, 0))(LocalTime.now())

  testUpdate[Boolean]("bit")(false)(true)

  testUpdate[String]("varchar(max)")("hi")("bye")

  testUpdate[UUID]("uniqueidentifier")(UUID.randomUUID())(UUID.randomUUID())

  /**
    * JTDS returns a value with a precision of about 4 ms,
    * so we can't use straight equality.
    *
    * http://sourceforge.net/p/jtds/feature-requests/73/
    */
  test("Update java.sql.Timestamp") {implicit connection =>
    Select[UpdateCount](s"CREATE TABLE tbl (id int identity PRIMARY KEY, v datetime2)").run()

    select"INSERT INTO tbl (v) VALUES (${new Timestamp(0)})".run()

    val after = Timestamp.from(Instant.now())

    for (row <- selectForUpdate"SELECT * FROM tbl".run()) {
      row("v") = after
      row.updateRow()
    }

    val maybeValue = Select[Option[Timestamp]]("SELECT v FROM tbl").run()

    assert(maybeValue.nonEmpty)

    assert(Math.abs(maybeValue.get.getTime - after.getTime) < 5)
  }

  /**
    * JTDS returns a value with a precision of about 4 ms,
    * so we can't use straight equality.
    *
    * http://sourceforge.net/p/jtds/feature-requests/73/
    */
  test("Update java.time.Instant") {implicit connection =>
    Select[UpdateCount](s"CREATE TABLE tbl (id int identity PRIMARY KEY, v datetime2)").run()

    update"INSERT INTO tbl (v) VALUES (${Instant.ofEpochMilli(0)})".run()

    val after = Instant.now()

    for (row <- selectForUpdate"SELECT * FROM tbl".run()) {
      row("v") = after
      row.updateRow()
    }

    val maybeValue = Select[Option[Instant]]("SELECT v FROM tbl").run()

    assert(maybeValue.nonEmpty)

    assert(Math.abs(maybeValue.get.toEpochMilli - after.toEpochMilli) < 5)
  }

  test(s"Update HierarchyId") {implicit connection =>
    val before = HierarchyId()
    val after = HierarchyId(1, 2)

    Select[UpdateCount](s"CREATE TABLE tbl (id int identity PRIMARY KEY, v hierarchyid)").run()

    update"INSERT INTO tbl (v) VALUES ($before)".run()

    for (row <- selectForUpdate"SELECT id, v FROM tbl".run()) {
      row("v") = after
      row.updateRow()
    }

    val maybeValue = Select[Option[HierarchyId]]("SELECT v.ToString() FROM tbl").run()

    assert(maybeValue.nonEmpty)

    assertResult(Some(after))(maybeValue)
  }

  test(s"Update None") {implicit connection =>
    val before = Some(1)
    val after = None

    Select[UpdateCount](s"CREATE TABLE tbl (id int identity PRIMARY KEY, v int)").run()

    update"INSERT INTO tbl (v) VALUES ($before)".run()

    for (row <- selectForUpdate"SELECT id, v FROM tbl".run()) {
      row("v") = after
      row.updateRow()
    }

    val maybeRow = Select[Option[Option[Int]]]("SELECT v FROM tbl").run()

    assert(maybeRow.nonEmpty, "There was a row")

    val maybeValue = maybeRow.get

    assert(maybeValue.isEmpty)
  }

}
