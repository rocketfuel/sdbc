package com.rocketfuel.sdbc.h2.implementation

import java.sql.Types
import java.util.UUID

import com.rocketfuel.sdbc.base.jdbc
import com.rocketfuel.sdbc.h2.Serialized
import org.h2.value.DataType
import scodec.bits.ByteVector

trait ArrayTypes {
  self: jdbc.DBMS with jdbc.SeqParameter =>

  private def nameOfJdbcType(jdbcType: Int): String = {
    DataType.getDataType(DataType.convertSQLTypeToValueType(jdbcType)).name
  }

  implicit val integerTypeName = ConcreteArrayType[Int](nameOfJdbcType(DataType.convertSQLTypeToValueType(Types.INTEGER)))

  implicit val booleanTypeName = ConcreteArrayType[Boolean](nameOfJdbcType(Types.BOOLEAN))

  implicit val tinyintTypeName = ConcreteArrayType[Byte](nameOfJdbcType(Types.TINYINT))

  implicit val smallintTypeName = ConcreteArrayType[Short](nameOfJdbcType(Types.SMALLINT))

  implicit val bigintTypeName = ConcreteArrayType[Long](nameOfJdbcType(Types.BIGINT))

  implicit val decimalTypeName = ConcreteArrayType[java.math.BigDecimal](nameOfJdbcType(Types.DECIMAL))

  implicit val floatTypeName = ConcreteArrayType[Float](nameOfJdbcType(Types.FLOAT))

  implicit val realTypeName = ConcreteArrayType[Double](nameOfJdbcType(Types.REAL))

  implicit val timeTypeName = ConcreteArrayType[java.sql.Time](nameOfJdbcType(Types.TIME))

  implicit val dateTypeName = ConcreteArrayType[java.sql.Date](nameOfJdbcType(Types.DATE))

  implicit val timestampTypeName = ConcreteArrayType[java.sql.Timestamp](nameOfJdbcType(Types.TIMESTAMP))

  implicit val varbinaryTypeName = ConcreteArrayType[ByteVector](nameOfJdbcType(Types.VARBINARY))

  implicit val otherTypeName = ConcreteArrayType[Serialized](nameOfJdbcType(Types.JAVA_OBJECT))

  implicit val varcharTypeName = ConcreteArrayType[String](nameOfJdbcType(Types.VARCHAR))

  implicit val uuidTypeName = ConcreteArrayType[UUID](DataType.getTypeByName("UUID").name)

}
