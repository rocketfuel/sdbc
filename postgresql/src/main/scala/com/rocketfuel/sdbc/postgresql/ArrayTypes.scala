package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.base.jdbc
import com.rocketfuel.sdbc.base.jdbc.statement.SeqParameter
import java.nio.ByteBuffer
import java.time._
import java.util.UUID
import argonaut._
import scodec.bits.ByteVector

trait ArrayTypes {
  self: jdbc.DBMS with SeqParameter =>

  implicit val smallintTypeName: ArrayTypeName[Short] =
    ArrayTypeName[Short]("int2")

  implicit val boxedSmallIntType: ArrayTypeName[java.lang.Short] =
    ArrayTypeName[java.lang.Short]("int2")

  implicit val integerTypeName: ArrayTypeName[Int] =
    ArrayTypeName[Int]("int4")

  implicit val boxedIntegerTypeName: ArrayTypeName[Integer] =
    ArrayTypeName[Integer]("int4")

  implicit val bigintTypeName: ArrayTypeName[Long] =
    ArrayTypeName[Long]("int8")

  implicit val boxedBigintTypeName: ArrayTypeName[java.lang.Long] =
    ArrayTypeName[java.lang.Long]("int8")

  implicit val booleanTypeName: ArrayTypeName[Boolean] =
    ArrayTypeName[Boolean]("bool")

  implicit val boxedBooleanTypeName: ArrayTypeName[java.lang.Boolean] =
    ArrayTypeName[java.lang.Boolean]("bool")

  implicit val javaBigDecimalTypeName: ArrayTypeName[java.math.BigDecimal] =
    ArrayTypeName[java.math.BigDecimal]("numeric")

  implicit val bigDecimalTypeName: ArrayTypeName[BigDecimal] =
    ArrayTypeName[BigDecimal]("numeric")

  implicit val floatTypeName: ArrayTypeName[Float] =
    ArrayTypeName[Float]("float4")

  implicit val boxedFloatTypeName: ArrayTypeName[java.lang.Float] =
    ArrayTypeName[java.lang.Float]("float4")

  implicit val doubleTypeName: ArrayTypeName[Double] =
    ArrayTypeName[Double]("float8")

  implicit val boxedDoubleTypeName: ArrayTypeName[java.lang.Double] =
    ArrayTypeName[java.lang.Double]("float8")

  implicit val timeTypeName: ArrayTypeName[java.sql.Time] =
    ArrayTypeName[java.sql.Time]("time")

  implicit val localTimeTypeName: ArrayTypeName[LocalTime] =
    ArrayTypeName[LocalTime]("time")

  implicit val dateTypeName: ArrayTypeName[java.sql.Date] =
    ArrayTypeName[java.sql.Date]("date")

  implicit val localDateTypeName: ArrayTypeName[LocalDate] =
    ArrayTypeName[LocalDate]("date")

  implicit val timestampTypeName: ArrayTypeName[java.sql.Timestamp] =
    ArrayTypeName[java.sql.Timestamp]("timestamp")

  implicit val instantTypeName: ArrayTypeName[Instant] =
    ArrayTypeName[Instant]("timestamp")

  implicit val timestampTzTypeName: ArrayTypeName[OffsetDateTime] =
    ArrayTypeName[OffsetDateTime]("timestamptz")

  implicit val timeTzTypeName: ArrayTypeName[OffsetTime] =
    ArrayTypeName[OffsetTime]("timetz")

  implicit val varbinaryTypeName: ArrayTypeName[ByteVector] =
    ArrayTypeName[ByteVector]("bytea")

  implicit val bytesTypeName: ArrayTypeName[Array[Byte]] =
    ArrayTypeName[Array[Byte]]("bytea")

  implicit val byteBufferTypesName: ArrayTypeName[ByteBuffer] =
    ArrayTypeName[ByteBuffer]("bytea")

  implicit val varcharTypeName: ArrayTypeName[String] =
    ArrayTypeName[String]("text")

  implicit val uuidTypeName: ArrayTypeName[UUID] =
    ArrayTypeName[UUID]("uuid")

}
