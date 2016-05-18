package com.rocketfuel.sdbc.postgresql

import java.sql.{Date, Time, Timestamp}
import java.time._
import java.util.UUID
import org.apache.commons.lang3.RandomStringUtils
import org.json4s.JValue
import org.json4s.jackson.JsonMethods
import scala.xml.NodeSeq
import scodec.bits.ByteVector
import scala.reflect.ClassTag
import com.rocketfuel.sdbc.PostgreSql._

class UpdatersSpec
  extends PostgreSqlSuite {

  def testUpdate[T](
    typeName: String
  )(before: T
  )(after: T
  )(implicit ctag: ClassTag[T],
    updater: Updater[T],
    setter: T => ParameterValue,
    converter: CompositeGetter[Row, T]
  ): Unit = {
    test(s"Update ${ctag.runtimeClass.getName}") {implicit connection =>
      val tableName = RandomStringUtils.randomAlphabetic(10)

      Select[UpdateCount](s"CREATE TABLE $tableName (id serial PRIMARY KEY, v $typeName)").run()

      Select[UpdateCount](s"INSERT INTO $tableName (v) VALUES (@before)").on("before" -> before).run()

      for (row <- Select[CloseableIterator[UpdatableRow]](s"SELECT * FROM $tableName").run()) {
        row("v") = after
        row.updateRow()
      }

      val maybeValue = Select[Option[T]](s"SELECT v FROM $tableName").run()

      assert(maybeValue.nonEmpty)

      (after, maybeValue.get) match {
        case (a: Array[_], b: Array[_]) =>
          assert(a.sameElements(b))
        case (expectedAfter, actualAfter) =>
          assertResult(expectedAfter)(actualAfter)
      }


    }
  }

  testUpdate[Long]("int8")(1L)(2L)

  testUpdate[Int]("int4")(1)(2)

  testUpdate[Short]("int2")(1.toShort)(2.toShort)

  testUpdate[Double]("float8")(1.0)(2.0)

  testUpdate[Float]("float4")(1.0F)(2.0F)

  testUpdate[java.lang.Long]("int8")(1L)(2L)

  testUpdate[java.lang.Integer]("int4")(1)(2)

  testUpdate[java.lang.Short]("int2")(1.toShort)(2.toShort)

  testUpdate[java.lang.Double]("float8")(1.0)(2.0)

  testUpdate[java.lang.Float]("float4")(1.0F)(2.0F)

  testUpdate[ByteVector]("bytea")(ByteVector(1, 2, 3))(ByteVector(4, 5, 6))

  testUpdate[Array[Byte]]("bytea")(Array[Byte](1, 2, 3))(Array[Byte](4, 5, 6))

  testUpdate[BigDecimal]("numeric")(BigDecimal(3))(BigDecimal("500"))

  testUpdate[Timestamp]("timestamp")(new Timestamp(0))(Timestamp.from(Instant.now()))

  testUpdate[Date]("date")(new Date(0))(Date.valueOf(LocalDate.now()))

  testUpdate[Time]("time")(new Time(0))(Time.valueOf(LocalTime.now()))

  testUpdate[Instant]("timestamp")(Instant.ofEpochMilli(0))(Instant.now())

  testUpdate[LocalDate]("date")(LocalDate.ofEpochDay(0))(LocalDate.now())

  testUpdate[LocalTime]("time")(LocalTime.of(0, 0, 0))(LocalTime.now())

  testUpdate[Boolean]("bool")(false)(true)

  testUpdate[String]("text")("hi")("bye")

  testUpdate[UUID]("uuid")(UUID.randomUUID())(UUID.randomUUID())

  testUpdate[Map[String, String]]("hstore")(Map("hi" -> "there"))(Map("bye" -> "now"))

  testUpdate[NodeSeq]("xml")(<a></a>)(<b></b>)

  testUpdate[JValue]("json")(JsonMethods.parse("{}"))(JsonMethods.parse("""{"a": 1}"""))

  test(s"Update None") {implicit connection =>
    val before = Some(1)
    val after = None

    Select[UpdateCount](s"CREATE TABLE tbl (id serial PRIMARY KEY, v int)").run()

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
