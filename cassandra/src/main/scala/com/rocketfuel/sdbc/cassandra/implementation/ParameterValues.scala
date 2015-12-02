package com.rocketfuel.sdbc.cassandra.implementation

import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Date, UUID}
import com.datastax.driver.core._
import com.rocketfuel.sdbc.base
import scodec.bits.ByteVector
import scala.collection.convert.decorateAsJava._

private[sdbc] trait ParameterValues {

  type IsParameter[T] = base.IsParameter[T, BoundStatement, Int]

  implicit def BooleanToParameter(value: Boolean): ParameterValue = {
    ParameterValue(value)
  }

  implicit def BoxedBooleanToParameter(value: java.lang.Boolean): ParameterValue = {
    Boolean.unbox(value)
  }

  implicit def ByteVectorToParameter(value: ByteVector): ParameterValue = {
    ParameterValue(value)
  }

  implicit def ByteBufferToParameter(value: ByteBuffer): ParameterValue = {
    ParameterValue(ByteVector(value))
  }

  implicit def ArrayByteToParameter(value: Array[Byte]): ParameterValue = {
    ParameterValue(ByteVector(value))
  }

  implicit def DateToParameter(value: Date): ParameterValue = {
    ParameterValue(value)
  }

  implicit def BigDecimalToParameter(value: BigDecimal): ParameterValue = {
    value.underlying()
  }

  implicit def JavaBigDecimalToParameter(value: java.math.BigDecimal): ParameterValue = {
    ParameterValue(value)
  }

  implicit def DoubleToParameter(value: Double): ParameterValue = {
    ParameterValue(value)
  }

  implicit def BoxedDoubleToParameter(value: java.lang.Double): ParameterValue = {
    ParameterValue(value)
  }

  implicit def FloatToParameter(value: Float): ParameterValue = {
    ParameterValue(value)
  }

  implicit def BoxedFloatToParameter(value: java.lang.Float): ParameterValue = {
    ParameterValue(value)
  }

  implicit def InetAddressToParameter(value: InetAddress): ParameterValue = {
    ParameterValue(value)
  }

  implicit def IntToParameter(value: Int): ParameterValue = {
    ParameterValue(value)
  }

  implicit def BoxedIntToParameter(value: java.lang.Integer): ParameterValue = {
    ParameterValue(value)
  }

  implicit def JavaListToParameter[T](value: java.util.List[T]): ParameterValue = {
    ParameterValue(value)
  }

  implicit def SeqToParameter[_](value: Seq[_]): ParameterValue = {
    value.asJava
  }

  implicit def LongToParameter(value: Long): ParameterValue = {
    ParameterValue(value)
  }

  implicit def BoxedLongToParameter(value: java.lang.Long): ParameterValue = {
    ParameterValue(value)
  }

  implicit def JavaMapToParameter(value: java.util.Map[_, _]): ParameterValue = {
    ParameterValue(value)
  }

  implicit def MapToParameter(value: Map[_, _]): ParameterValue = {
    value.asJava
  }

  implicit def JavaSetToParameter(value: java.util.Set[_]): ParameterValue = {
    ParameterValue(value)
  }

  implicit def SetToParameter(value: Set[_]): ParameterValue = {
    value.asJava
  }

  implicit def StringToParameter(value: String): ParameterValue = {
    ParameterValue(value)
  }

  implicit def UUIDToParameter(value: UUID): ParameterValue = {
    ParameterValue(value)
  }

  implicit def TokenToParameter(value: Token): ParameterValue = {
    ParameterValue(value)
  }

  implicit def TupleValueToParameter(value: TupleValue): ParameterValue = {
    ParameterValue(value)
  }

  implicit def UDTValueToParameter(value: UDTValue): ParameterValue = {
    ParameterValue(value)
  }

  implicit def BigIntegerToParameter(value: BigInteger): ParameterValue = {
    ParameterValue(value)
  }

}
