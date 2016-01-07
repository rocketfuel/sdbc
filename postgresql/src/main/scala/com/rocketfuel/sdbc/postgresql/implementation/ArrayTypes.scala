package com.rocketfuel.sdbc.postgresql.implementation

import com.rocketfuel.sdbc.base.jdbc
import java.nio.ByteBuffer
import java.time._
import java.util.UUID
import org.json4s._
import scodec.bits.ByteVector

private[sdbc] trait ArrayTypes {
  self: jdbc.DBMS with jdbc.SeqParameter =>

  implicit val smallintTypeName = ArrayTypeName[Short]("int2")

  implicit val boxedSmallIntType = ArrayTypeName[java.lang.Short]("int2")

  implicit val integerTypeName = ArrayTypeName[Int]("int4")

  implicit val boxedIntegerTypeName = ArrayTypeName[Integer]("int4")

  implicit val bigintTypeName = ArrayTypeName[Long]("int8")

  implicit val boxedBigintTypeName = ArrayTypeName[java.lang.Long]("int8")

  implicit val booleanTypeName = ArrayTypeName[Boolean]("bool")

  implicit val boxedBooleanTypeName = ArrayTypeName[java.lang.Boolean]("bool")

  implicit val javaBigDecimalTypeName = ArrayTypeName[java.math.BigDecimal]("numeric")

  implicit val bigDecimalTypeName = ArrayTypeName[BigDecimal]("numeric")

  implicit val floatTypeName = ArrayTypeName[Float]("float4")

  implicit val boxedFloatTypeName = ArrayTypeName[java.lang.Float]("float4")

  implicit val doubleTypeName = ArrayTypeName[Double]("float8")

  implicit val boxedDoubleTypeName = ArrayTypeName[java.lang.Double]("float8")

  implicit val timeTypeName = ArrayTypeName[java.sql.Time]("time")

  implicit val localTimeTypeName = ArrayTypeName[LocalTime]("time")

  implicit val dateTypeName = ArrayTypeName[java.sql.Date]("date")

  implicit val localDateTypeName = ArrayTypeName[LocalDate]("date")

  implicit val timestampTypeName = ArrayTypeName[java.sql.Timestamp]("timestamp")

  implicit val instantTypeName = ArrayTypeName[Instant]("timestamp")

  implicit val timestampTzTypeName = ArrayTypeName[OffsetDateTime]("timestamptz")

  implicit val timeTzTypeName = ArrayTypeName[OffsetTime]("timetz")

  implicit val varbinaryTypeName = ArrayTypeName[ByteVector]("bytea")

  implicit val bytesTypeName = ArrayTypeName[Array[Byte]]("bytea")

  implicit val byteBufferTypesName = ArrayTypeName[ByteBuffer]("bytea")

  implicit val varcharTypeName = ArrayTypeName[String]("text")

  implicit val jsonTypeName = ArrayTypeName[JValue]("json")

  implicit val uuidTypeName = ArrayTypeName[UUID]("uuid")

}
