package com.rocketfuel.sdbc.cassandra

import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.api.core.metadata.token.Token
import com.rocketfuel.sdbc.base
import java.lang
import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteBuffer
import java.time.{Instant, LocalDate, LocalTime}
import java.util.UUID
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scodec.bits.ByteVector
import shapeless.HList
import shapeless.ops.hlist.{Mapper, ToTraversable}
import shapeless.ops.product.ToHList

trait ParameterValue
  extends base.ParameterValue
  with base.CompiledParameterizedQuery {

  override type PreparedStatement = com.datastax.oss.driver.api.core.cql.BoundStatementBuilder

  override protected def setNone(preparedStatement: PreparedStatement, parameterIndex: Int): PreparedStatement = {
    preparedStatement.setToNull(parameterIndex)
    preparedStatement
  }

  implicit val BooleanParameter: Parameter[Boolean] = {
    (value: Boolean) => (statement: PreparedStatement, ix: Int) =>
      statement.setBoolean(ix, value)
  }

  implicit val BoxedBooleanParameter: Parameter[lang.Boolean] = Parameter.derived[lang.Boolean, Boolean]

  //We're using ByteVectors, since they're much more easily testable than Array[Byte].
  //IE equality actually works. Also, they're immutable.
  implicit val ByteVectorParameter: Parameter[ByteVector] = {
    (value: ByteVector) =>
      val bufferValue = value.toByteBuffer
      (statement: PreparedStatement, parameterIndex: Int) =>
        statement.setByteBuffer(parameterIndex, bufferValue)
  }

  implicit val ByteBufferParameter: Parameter[ByteBuffer] = Parameter.converted[ByteBuffer, ByteVector](ByteVector(_))

  implicit val ArrayByteParameter: Parameter[Array[Byte]] = Parameter.converted[Array[Byte], ByteVector](ByteVector(_))

  implicit val SeqByteParameter: Parameter[Seq[Byte]] = Parameter.converted[Seq[Byte], ByteVector](ByteVector(_))

  implicit val LocalDateParameter: Parameter[LocalDate] = {
    (value: LocalDate) => (statement: PreparedStatement, parameterIndex: Int) =>
      statement.setLocalDate(parameterIndex, value)
  }

  implicit val LocalTimeParameter: Parameter[LocalTime] = {
    (value: LocalTime) => (statement: PreparedStatement, parameterIndex: Int) =>
      statement.setLocalTime(parameterIndex, value)
  }

  implicit val InstantParameter: Parameter[Instant] = {
    (value: Instant) => (statement: PreparedStatement, parameterIndex: Int) =>
      statement.setInstant(parameterIndex, value)
  }

  implicit val JavaBigDecimalParameter: Parameter[java.math.BigDecimal] = {
    (value: java.math.BigDecimal) => (statement: PreparedStatement, parameterIndex: Int) =>
      statement.setBigDecimal(parameterIndex, value)
  }

  implicit val BigDecimalParameter: Parameter[BigDecimal] =
    Parameter.converted[BigDecimal, java.math.BigDecimal](_.underlying())

  implicit val DoubleParameter: Parameter[Double] = {
    (value: Double) => (statement: PreparedStatement, ix: Int) =>
      statement.setDouble(ix, value)
  }

  implicit val BoxedDoubleParameter: Parameter[lang.Double] = Parameter.derived[lang.Double, Double]

  implicit val FloatParameter: Parameter[Float] = {
    (value: Float) => (statement: PreparedStatement, ix: Int) =>
      statement.setFloat(ix, value)
  }

  implicit val BoxedFloatParameter: Parameter[lang.Float] = Parameter.derived[lang.Float, Float]

  implicit val InetAddressParameter: Parameter[InetAddress] = {
    (value: InetAddress) => (statement: PreparedStatement, ix: Int) =>
      statement.setInetAddress(ix, value)
  }

  implicit val IntParameter: Parameter[Int] = {
    (value: Int) => (statement: PreparedStatement, ix: Int) =>
      statement.setInt(ix, value)
  }

  implicit val BoxedIntParameter: Parameter[Integer] = Parameter.derived[Integer, Int]

  implicit def JavaSeqParameter[T](implicit c: ClassTag[T]): Parameter[java.util.List[T]] = {
    (value: java.util.List[T]) => (statement: PreparedStatement, ix: Int) =>
      statement.setList[T](ix, value, c.runtimeClass.asInstanceOf[Class[T]])
  }

  implicit def SeqParameter[T](implicit c: ClassTag[T]): Parameter[Seq[T]] =
    Parameter.converted[Seq[T], java.util.List[T]](_.asJava)

  implicit val LongParameter: Parameter[Long] = {
    (value: Long) => (statement: PreparedStatement, ix: Int) =>
      statement.setLong(ix, value)
  }

  implicit val BoxedLongParameter: Parameter[lang.Long] = Parameter.derived[lang.Long, Long]

  implicit def JavaMapParameter[Key, Value](implicit k: ClassTag[Key], v: ClassTag[Value]): Parameter[java.util.Map[Key, Value]] = {
    (value: java.util.Map[Key, Value]) => (statement: PreparedStatement, ix: Int) =>
      statement.setMap[Key, Value](ix, value, k.runtimeClass.asInstanceOf[Class[Key]], v.runtimeClass.asInstanceOf[Class[Value]])
  }

  implicit def MapParameter[Key, Value](implicit k: ClassTag[Key], v: ClassTag[Value]): Parameter[Map[Key, Value]] =
    Parameter.converted[Map[Key, Value], java.util.Map[Key, Value]](_.asJava)

  implicit def JavaSetParameter[T](implicit c: ClassTag[T]): Parameter[java.util.Set[T]] = {
    (value: java.util.Set[T]) => (statement: PreparedStatement, ix: Int) =>
      statement.setSet[T](ix, value, c.runtimeClass.asInstanceOf[Class[T]])
  }

  implicit def SetParameter[T](implicit c: ClassTag[T]): Parameter[Set[T]] =
    Parameter.converted[Set[T], java.util.Set[T]](_.asJava)

  implicit val StringParameter: Parameter[String] = {
    (value: String) => (statement: PreparedStatement, ix: Int) =>
      statement.setString(ix, value)
  }

  implicit val UUIDParameter: Parameter[UUID] = {
    (value: UUID) => (statement: PreparedStatement, ix: Int) =>
      statement.setUuid(ix, value)
  }

  implicit val TokenParameter: Parameter[Token] = {
    (value: Token) => (statement: PreparedStatement, ix: Int) =>
      statement.setToken(ix, value)
  }

  implicit val TupleValueParameter: Parameter[TupleValue] = {
    (value: TupleValue) => (statement: PreparedStatement, ix: Int) =>
      statement.setTupleValue(ix, value.underlying)
  }

  implicit val UDTValueParameter: Parameter[UdtValue] = {
    (value: UdtValue) => (statement: PreparedStatement, ix: Int) =>
      statement.setUdtValue(ix, value)
  }

  implicit val BigIntegerParameter: Parameter[BigInteger] = {
    (value: BigInteger) => (statement: PreparedStatement, ix: Int) =>
      statement.setBigInteger(ix, value)
  }

  implicit def hlistParameterValue[
    H <: HList,
    ListH <: HList,
    MappedTypesH <: HList,
    MappedValuesH <: HList
  ](h: H
  )(implicit dataTypeMapper: Mapper.Aux[TupleDataType.ToDataType.type, H, MappedTypesH],
    dataTypeList: ToTraversable.Aux[MappedTypesH, Seq, DataType],
    dataValueMapper: Mapper.Aux[TupleDataType.ToDataValue.type, H, MappedValuesH],
    dataValueList: ToTraversable.Aux[MappedValuesH, Seq, AnyRef]
  ): ParameterValue = {
    TupleValue.hlistTupleValue(h)
  }

  implicit def productParameterValue[
    P,
    H <: HList,
    ListH <: HList,
    MappedTypesH <: HList,
    MappedValuesH <: HList
  ](p: P
  )(implicit toHList: ToHList.Aux[P, H],
    dataTypeMapper: Mapper.Aux[TupleDataType.ToDataType.type, H, MappedTypesH],
    dataTypeList: ToTraversable.Aux[MappedTypesH, Seq, DataType],
    dataValueMapper: Mapper.Aux[TupleDataType.ToDataValue.type, H, MappedValuesH],
    dataValueList: ToTraversable.Aux[MappedValuesH, Seq, AnyRef],
    toParameterValue: TupleValue => ParameterValue
  ): ParameterValue = {
    val asTupleValue = TupleValue.productTupleValue(p)
    toParameterValue(asTupleValue)
  }

}
