package com.rocketfuel.sdbc.h2.implementation

import com.rocketfuel.sdbc.base.jdbc.statement.SeqParameter
import java.nio.ByteBuffer
import java.sql.Types
import java.time.{Instant, LocalDate, LocalTime}
import java.util.UUID

import com.rocketfuel.sdbc.base.jdbc
import com.rocketfuel.sdbc.h2.Serialized
import org.h2.value.DataType
import scodec.bits.ByteVector

private[sdbc] trait ArrayTypes {
  self: jdbc.DBMS with SeqParameter =>

  private def nameOfJdbcType(jdbcType: Int): String = {
    DataType.getDataType(DataType.convertSQLTypeToValueType(jdbcType)).name
  }

  implicit val integerTypeName = ArrayTypeName[Int](nameOfJdbcType(Types.INTEGER))

  implicit val boxedIntegerTypeName = ArrayTypeName[Integer](nameOfJdbcType(Types.INTEGER))

  implicit val booleanTypeName = ArrayTypeName[Boolean](nameOfJdbcType(Types.BOOLEAN))

  implicit val boxedBooleanTypeName = ArrayTypeName[java.lang.Boolean](nameOfJdbcType(Types.BOOLEAN))

  implicit val tinyintTypeName = ArrayTypeName[Byte](nameOfJdbcType(Types.TINYINT))

  implicit val boxedTinyIntTypeName = ArrayTypeName[java.lang.Byte](nameOfJdbcType(Types.TINYINT))

  implicit val smallintTypeName = ArrayTypeName[Short](nameOfJdbcType(Types.SMALLINT))

  implicit val boxedSmallIntType = ArrayTypeName[java.lang.Short](nameOfJdbcType(Types.SMALLINT))

  implicit val bigintTypeName = ArrayTypeName[Long](nameOfJdbcType(Types.BIGINT))

  implicit val boxedBigintTypeName = ArrayTypeName[java.lang.Long](nameOfJdbcType(Types.BIGINT))

  implicit val javaDecimalTypeName = ArrayTypeName[java.math.BigDecimal](nameOfJdbcType(Types.DECIMAL))

  implicit val decimalTypeName = ArrayTypeName[BigDecimal](nameOfJdbcType(Types.DECIMAL))

  implicit val floatTypeName = ArrayTypeName[Float](nameOfJdbcType(Types.FLOAT))

  implicit val boxedFloatTypeName = ArrayTypeName[java.lang.Float](nameOfJdbcType(Types.FLOAT))

  implicit val realTypeName = ArrayTypeName[Double](nameOfJdbcType(Types.REAL))

  implicit val boxedDoubleTypeName = ArrayTypeName[java.lang.Double](nameOfJdbcType(Types.REAL))

  implicit val timeTypeName = ArrayTypeName[java.sql.Time](nameOfJdbcType(Types.TIME))

  implicit val localTimeTypeName = ArrayTypeName[LocalTime](nameOfJdbcType(Types.TIME))

  implicit val dateTypeName = ArrayTypeName[java.sql.Date](nameOfJdbcType(Types.DATE))

  implicit val localDateTypeName = ArrayTypeName[LocalDate](nameOfJdbcType(Types.DATE))

  implicit val timestampTypeName = ArrayTypeName[java.sql.Timestamp](nameOfJdbcType(Types.TIMESTAMP))

  implicit val instantTypeName = ArrayTypeName[Instant](nameOfJdbcType(Types.TIMESTAMP))

  implicit val varbinaryTypeName = ArrayTypeName[ByteVector](nameOfJdbcType(Types.VARBINARY))

  implicit val bytesTypeName = ArrayTypeName[Array[Byte]](nameOfJdbcType(Types.VARBINARY))

  implicit val byteBufferTypesName = ArrayTypeName[ByteBuffer](nameOfJdbcType(Types.VARBINARY))

  implicit val otherTypeName = ArrayTypeName[Serialized](nameOfJdbcType(Types.JAVA_OBJECT))

  implicit val varcharTypeName = ArrayTypeName[String](nameOfJdbcType(Types.VARCHAR))

  implicit val uuidTypeName = ArrayTypeName[UUID](DataType.getTypeByName("UUID").name)

}
