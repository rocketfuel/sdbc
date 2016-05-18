package com.rocketfuel.sdbc.h2

import java.sql.{Date, Time, Timestamp}
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalTime}
import java.util.UUID
import scodec.bits.ByteVector
import shapeless._
import shapeless.ops.hlist._
import scala.reflect.ClassTag
import com.rocketfuel.sdbc.H2._

class UpdatersSpec extends H2Suite {

  def testUpdate[T, MappedT <: HList](typeName: String)(before: T)(after: T)(implicit ctag: ClassTag[T], updater: Updater[T], converter: CompositeGetter[T], mapper: Mapper.Aux[ToParameterValue.type, T :: HNil, MappedT],
    toList: ToList[MappedT, ParameterValue]): Unit = {
    test(s"Update ${ctag.runtimeClass.getName}") {implicit connection =>
      Update(s"CREATE TABLE tbl (id identity PRIMARY KEY, v $typeName)").update()

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

  testUpdate("int8")(1L)(2L)

  testUpdate("int4")(1)(2)

  testUpdate("int2")(1.toShort)(2.toShort)

  testUpdate("tinyint")(1.toByte)(2.toByte)

  testUpdate("float8")(1.0)(2.0)

  testUpdate("float4")(1.0F)(2.0F)

  testUpdate("int8")(Long.box(1L))(Long.box(2L))

  testUpdate("int4")(Int.box(1))(Int.box(2))

  testUpdate("int2")(Short.box(1.toShort))(Short.box(2.toShort))

  testUpdate("tinyint")(Byte.box(1.toByte))(Byte.box(2.toByte))

  testUpdate("float8")(Double.box(1.0))(Double.box(2.0))

  testUpdate("float4")(Float.box(1.0F))(Float.box(2.0F))

  testUpdate("bytea")(ByteVector(1, 2, 3))(ByteVector(4, 5, 6))

  testUpdate("numeric")(BigDecimal(3))(BigDecimal("500"))

  testUpdate("timestamp")(new Timestamp(0))(Timestamp.from(Instant.now()))

  testUpdate("date")(new Date(0))(Date.valueOf(LocalDate.now()))

  testUpdate("time")(new Time(0))(Time.valueOf(LocalTime.now()))

  testUpdate("timestamp")(Instant.ofEpochMilli(0))(Instant.now())

  testUpdate("date")(LocalDate.ofEpochDay(0))(LocalDate.now())

  //H2 doesn't store fractional seconds.
  testUpdate("time")(LocalTime.of(0, 0, 0))(LocalTime.now().truncatedTo(ChronoUnit.SECONDS))

  testUpdate("bool")(false)(true)

  testUpdate("text")("hi")("bye")

  testUpdate("uuid")(UUID.randomUUID())(UUID.randomUUID())

  test(s"Update None") {implicit connection =>
    val before = Some(1)
    val after = None

    Update(s"CREATE TABLE tbl (id identity PRIMARY KEY, v int)").update()

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
