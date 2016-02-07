package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core
import com.datastax.driver.core.BoundStatement
import java.lang
import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteBuffer
import java.time.Instant
import java.util.{Date, UUID}
import com.rocketfuel.sdbc.base
import scodec.bits.ByteVector
import scala.collection.convert.decorateAsJava._

private[sdbc] trait ParameterValue
  extends base.ParameterValue
  with base.ParameterizedQuery {
  self: Cassandra =>

  type PreparedStatement = core.BoundStatement

  type Connection = core.Session

  override protected def setNone(preparedStatement: BoundStatement, parameterIndex: Int): BoundStatement = {
    preparedStatement.setToNull(parameterIndex)
    preparedStatement
  }

  implicit val BooleanParameter: Parameter[Boolean] = {
    (value: Boolean) => (statement: PreparedStatement, ix: Int) =>
      statement.setBool(ix, value)
  }

  implicit val BoxedBooleanParameter = DerivedParameter[lang.Boolean, Boolean]

  //We're using ByteVectors, since they're much more easily testable than Array[Byte].
  //IE equality actually works. Also, they're immutable.
  implicit val ByteVectorParameter: Parameter[ByteVector] = {
    (value: ByteVector) =>
      val bufferValue = value.toByteBuffer
      (statement: PreparedStatement, parameterIndex: Int) =>
        statement.setBytes(parameterIndex, bufferValue)
  }

  implicit val ByteBufferParameter = DerivedParameter[ByteBuffer, ByteVector](ByteVector.apply, ByteVectorParameter)

  implicit val ArrayByteParameter = DerivedParameter[Array[Byte], ByteVector](ByteVector.apply, ByteVectorParameter)

  implicit val SeqByteParameter = DerivedParameter[Seq[Byte], ByteVector](ByteVector.apply, ByteVectorParameter)

  implicit val DateParameter: Parameter[Date] = {
    (value: Date) => (statement: PreparedStatement, parameterIndex: Int) =>
      statement.setDate(parameterIndex, value)
  }

  implicit val InstantParameter = DerivedParameter[Instant, Date](Date.from, DateParameter)

  implicit val JavaBigDecimalParameter: Parameter[java.math.BigDecimal] = {
    (value: java.math.BigDecimal) => (statement: PreparedStatement, parameterIndex: Int) =>
      statement.setDecimal(parameterIndex, value)
  }

  implicit val DecimalToParameter = DerivedParameter[BigDecimal, java.math.BigDecimal](_.underlying(), JavaBigDecimalParameter)

  implicit val DoubleParameter: Parameter[Double] = {
    (value: Double) => (statement: PreparedStatement, ix: Int) =>
      statement.setDouble(ix, value)
  }

  implicit val BoxedDoubleParameter = DerivedParameter[lang.Double, Double]

  implicit val FloatParameter: Parameter[Float] = {
    (value: Float) => (statement: PreparedStatement, ix: Int) =>
      statement.setFloat(ix, value)
  }

  implicit val BoxedFloatParameter = DerivedParameter[lang.Float, Float]

  implicit val InetAddressParameter: Parameter[InetAddress] = {
    (value: InetAddress) => (statement: PreparedStatement, ix: Int) =>
      statement.setInet(ix, value)
  }

  implicit val IntParameter: Parameter[Int] = {
    (value: Int) => (statement: PreparedStatement, ix: Int) =>
      statement.setInt(ix, value)
  }

  implicit val BoxedIntParameter = DerivedParameter[Integer, Int]

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

  implicit val BoxedLongParameter = DerivedParameter[lang.Long, Long]

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

}
