package com.rocketfuel.sdbc.cassandra

import com.datastax.driver.core
import com.datastax.driver.core.LocalDate
import com.rocketfuel.sdbc.base
import java.lang
import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteBuffer
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.{Date, UUID}
import scala.collection.JavaConverters._
import scodec.bits.ByteVector
import shapeless.HList
import shapeless.ops.hlist.{Mapper, ToTraversable}
import shapeless.ops.product.ToHList

trait ParameterValue
  extends base.ParameterValue
  with base.CompiledParameterizedQuery {

  type PreparedStatement = core.BoundStatement

  override protected def setNone(preparedStatement: PreparedStatement, parameterIndex: Int): PreparedStatement = {
    preparedStatement.setToNull(parameterIndex)
    preparedStatement
  }

  implicit val BooleanParameter: Parameter[Boolean] = {
    (value: Boolean) => (statement: PreparedStatement, ix: Int) =>
      statement.setBool(ix, value)
  }

  implicit val BoxedBooleanParameter: Parameter[lang.Boolean] = Parameter.derived[lang.Boolean, Boolean]

  //We're using ByteVectors, since they're much more easily testable than Array[Byte].
  //IE equality actually works. Also, they're immutable.
  implicit val ByteVectorParameter: Parameter[ByteVector] = {
    (value: ByteVector) =>
      val bufferValue = value.toByteBuffer
      (statement: PreparedStatement, parameterIndex: Int) =>
        statement.setBytes(parameterIndex, bufferValue)
  }

  implicit val ByteBufferParameter: Parameter[ByteBuffer] = Parameter.converted[ByteBuffer, ByteVector](ByteVector(_))

  implicit val ArrayByteParameter: Parameter[Array[Byte]] = Parameter.converted[Array[Byte], ByteVector](ByteVector(_))

  implicit val SeqByteParameter: Parameter[Seq[Byte]] = Parameter.converted[Seq[Byte], ByteVector](ByteVector(_))

  implicit val LocalDateParameter: Parameter[LocalDate] = {
    (value: LocalDate) => (statement: PreparedStatement, parameterIndex: Int) =>
      statement.setDate(parameterIndex, value)
  }

  implicit val JavaLocalDateParameter: Parameter[java.time.LocalDate] =
    Parameter.converted[java.time.LocalDate, LocalDate](l => LocalDate.fromMillisSinceEpoch(TimeUnit.DAYS.toMillis(l.toEpochDay)))

  implicit val DateParameter: Parameter[Date] = {
    Parameter.converted[Date, LocalDate](d => LocalDate.fromMillisSinceEpoch(d.getTime))
  }

  implicit val InstantParameter: Parameter[Instant] = {
    Parameter.converted[Instant, LocalDate](i => LocalDate.fromMillisSinceEpoch(i.toEpochMilli))
  }

  implicit val JavaBigDecimalParameter: Parameter[java.math.BigDecimal] = {
    (value: java.math.BigDecimal) => (statement: PreparedStatement, parameterIndex: Int) =>
      statement.setDecimal(parameterIndex, value)
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
      statement.setInet(ix, value)
  }

  implicit val IntParameter: Parameter[Int] = {
    (value: Int) => (statement: PreparedStatement, ix: Int) =>
      statement.setInt(ix, value)
  }

  implicit val BoxedIntParameter: Parameter[Integer] = Parameter.derived[Integer, Int]

  implicit def JavaSeqParameter[T]: Parameter[java.util.List[T]] = {
    (value: java.util.List[T]) => (statement: PreparedStatement, ix: Int) =>
      statement.setList[T](ix, value)
  }

  implicit def SeqParameter[T]: Parameter[Seq[T]] = {
    (value: Seq[T]) =>
      val asJava = value.asJava
      (statement: PreparedStatement, ix: Int) =>
        statement.setList[T](ix, asJava)
  }

  implicit val LongParameter: Parameter[Long] = {
    (value: Long) => (statement: PreparedStatement, ix: Int) =>
      statement.setLong(ix, value)
  }

  implicit val BoxedLongParameter: Parameter[lang.Long] = Parameter.derived[lang.Long, Long]

  implicit def JavaMapParameter[Key, Value]: Parameter[java.util.Map[Key, Value]] = {
    (value: java.util.Map[Key, Value]) => (statement: PreparedStatement, ix: Int) =>
        statement.setMap[Key, Value](ix, value)
  }

  implicit def MapParameter[Key, Value]: Parameter[Map[Key, Value]] = {
    (value: Map[Key, Value]) =>
      val asJava = value.asJava
      (statement: PreparedStatement, ix: Int) =>
        statement.setMap[Key, Value](ix, asJava)
  }

  implicit def JavaSetParameter[T]: Parameter[java.util.Set[T]] = {
    (value: java.util.Set[T]) => (statement: PreparedStatement, ix: Int) =>
        statement.setSet[T](ix, value)
  }

  implicit def SetParameter[T]: Parameter[Set[T]] = {
    (value: Set[T]) =>
      val asJava = value.asJava
      (statement: PreparedStatement, ix: Int) =>
        statement.setSet[T](ix, asJava)
  }

  implicit val StringParameter: Parameter[String] = {
    (value: String) => (statement: PreparedStatement, ix: Int) =>
      statement.setString(ix, value)
  }

  implicit val UUIDParameter: Parameter[UUID] = {
    (value: UUID) => (statement: PreparedStatement, ix: Int) =>
      statement.setUUID(ix, value)
  }

  implicit val TokenParameter: Parameter[core.Token] = {
    (value: core.Token) => (statement: PreparedStatement, ix: Int) =>
      statement.setToken(ix, value)
  }

  implicit val TupleValueParameter: Parameter[TupleValue] = {
    (value: TupleValue) => (statement: PreparedStatement, ix: Int) =>
      statement.setTupleValue(ix, value.underlying)
  }

  implicit val UDTValueParameter: Parameter[core.UDTValue] = {
    (value: core.UDTValue) => (statement: PreparedStatement, ix: Int) =>
      statement.setUDTValue(ix, value)
  }

  implicit val BigIntegerParameter: Parameter[BigInteger] = {
    (value: BigInteger) => (statement: PreparedStatement, ix: Int) =>
      statement.setVarint(ix, value)
  }

  implicit def hlistParameterValue[
    H <: HList,
    ListH <: HList,
    MappedTypesH <: HList,
    MappedValuesH <: HList
  ](h: H
  )(implicit dataTypeMapper: Mapper.Aux[TupleDataType.ToDataType.type, H, MappedTypesH],
    dataTypeList: ToTraversable.Aux[MappedTypesH, Seq, core.DataType],
    dataValueMapper: Mapper.Aux[TupleDataType.ToDataValue.type, H, MappedValuesH],
    dataValueList: ToTraversable.Aux[MappedValuesH, Seq, AnyRef]
  ): ParameterValue = {
    TupleValue.hlistTupleValue(h)
  }

  implicit def productParameterValue[
    P <: Product,
    H <: HList,
    ListH <: HList,
    MappedTypesH <: HList,
    MappedValuesH <: HList
  ](p: P
  )(implicit toHList: ToHList.Aux[P, H],
    dataTypeMapper: Mapper.Aux[TupleDataType.ToDataType.type, H, MappedTypesH],
    dataTypeList: ToTraversable.Aux[MappedTypesH, Seq, core.DataType],
    dataValueMapper: Mapper.Aux[TupleDataType.ToDataValue.type, H, MappedValuesH],
    dataValueList: ToTraversable.Aux[MappedValuesH, Seq, AnyRef],
    toParameterValue: TupleValue => ParameterValue
  ): ParameterValue = {
    val asTupleValue = TupleValue.productTupleValue(p)
    toParameterValue(asTupleValue)
  }

}
