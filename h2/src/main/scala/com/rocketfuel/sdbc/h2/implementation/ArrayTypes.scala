package com.rocketfuel.sdbc.h2.implementation

import java.sql.Types
import java.util.UUID

import com.rocketfuel.sdbc.base.jdbc
import com.rocketfuel.sdbc.h2.Serialized
import org.h2.value.DataType
import scodec.bits.ByteVector

trait ArrayTypes {
  self: jdbc.DBMS with jdbc.SeqParameter =>

  private def h2DataTypeOfJdbcType(jdbcType: Int): String = {
    DataType.getDataType(DataType.convertSQLTypeToValueType(jdbcType)).name
  }

  implicit val integerTypeName = ConcreteArrayType[Int](h2DataTypeOfJdbcType(DataType.convertSQLTypeToValueType(Types.INTEGER)))

  implicit val booleanTypeName = ConcreteArrayType[Boolean](h2DataTypeOfJdbcType(Types.BOOLEAN))

  implicit val tinyintTypeName = ConcreteArrayType[Byte](h2DataTypeOfJdbcType(Types.TINYINT))

  implicit val smallintTypeName = ConcreteArrayType[Short](h2DataTypeOfJdbcType(Types.SMALLINT))

  implicit val bigintTypeName = ConcreteArrayType[Long](h2DataTypeOfJdbcType(Types.BIGINT))

  implicit val decimalTypeName = ConcreteArrayType[java.math.BigDecimal](h2DataTypeOfJdbcType(Types.DECIMAL))

  implicit val floatTypeName = ConcreteArrayType[Float](h2DataTypeOfJdbcType(Types.FLOAT))

  implicit val realTypeName = ConcreteArrayType[Double](h2DataTypeOfJdbcType(Types.REAL))

  implicit val timeTypeName = ConcreteArrayType[java.sql.Time](h2DataTypeOfJdbcType(Types.TIME))

  implicit val dateTypeName = ConcreteArrayType[java.sql.Date](h2DataTypeOfJdbcType(Types.DATE))

  implicit val timestampTypeName = ConcreteArrayType[java.sql.Timestamp](h2DataTypeOfJdbcType(Types.TIMESTAMP))

  implicit val varbinaryTypeName = ConcreteArrayType[ByteVector](h2DataTypeOfJdbcType(Types.VARBINARY))

  implicit val otherTypeName = ConcreteArrayType[Serialized](h2DataTypeOfJdbcType(Types.JAVA_OBJECT))

  implicit val varcharTypeName = ConcreteArrayType[String](h2DataTypeOfJdbcType(Types.VARCHAR))

  implicit val uuidTypeName = ConcreteArrayType[UUID](DataType.getTypeByName("UUID").name)

}
