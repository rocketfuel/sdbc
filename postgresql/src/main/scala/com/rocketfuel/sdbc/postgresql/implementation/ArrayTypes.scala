package com.rocketfuel.sdbc.postgresql.implementation

import com.rocketfuel.sdbc.base.jdbc
import java.time._
import java.util.UUID
import org.json4s._
import scodec.bits.ByteVector

private[sdbc] trait ArrayTypes {
  self: jdbc.DBMS with jdbc.SeqParameter =>

  implicit val smallintTypeName = ConcreteArrayType[Short]("int2")

  implicit val boxedSmallIntType = ConcreteArrayType[java.lang.Short]("int2")

  implicit val integerTypeName = ConcreteArrayType[Int]("int4")

  implicit val boxedIntegerTypeName = ConcreteArrayType[Integer]("int4")

  implicit val bigintTypeName = ConcreteArrayType[Long]("int8")

  implicit val boxedBigintTypeName = ConcreteArrayType[java.lang.Long]("int8")

  implicit val booleanTypeName = ConcreteArrayType[Boolean]("bool")

  implicit val boxedBooleanTypeName = ConcreteArrayType[java.lang.Boolean]("bool")

  implicit val javaBigDecimalTypeName = ConcreteArrayType[java.math.BigDecimal]("numeric")

  implicit val bigDecimalTypeName = ConcreteArrayType[BigDecimal]("numeric")

  implicit val floatTypeName = ConcreteArrayType[Float]("float4")

  implicit val boxedFloatTypeName = ConcreteArrayType[java.lang.Float]("float4")

  implicit val doubleTypeName = ConcreteArrayType[Double]("float8")

  implicit val boxedDoubleTypeName = ConcreteArrayType[java.lang.Double]("float8")

  implicit val timeTypeName = ConcreteArrayType[java.sql.Time]("time")

  implicit val localTimeTypeName = ConcreteArrayType[LocalTime]("time")

  implicit val dateTypeName = ConcreteArrayType[java.sql.Date]("date")

  implicit val timestampTypeName = ConcreteArrayType[java.sql.Timestamp]("timestamp")

  implicit val timestampTzTypeName = ConcreteArrayType[OffsetDateTime]("timestamptz")

  implicit val timeTzTypeName = ConcreteArrayType[OffsetTime]("timetz")

  implicit val varbinaryTypeName = ConcreteArrayType[ByteVector]("bytea")

  implicit val varcharTypeName = ConcreteArrayType[String]("text")

  implicit val jsonTypeName = ConcreteArrayType[JValue]("json")

  implicit val uuidTypeName = ConcreteArrayType[UUID]("uuid")

}
