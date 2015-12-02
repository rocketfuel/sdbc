package com.rocketfuel.sdbc.cassandra.implementation

import java.net.InetAddress

import com.datastax.driver.core._
import com.rocketfuel.sdbc.base
import scodec.bits.ByteVector

private[sdbc] object ParameterSetter extends base.ParameterSetter[BoundStatement, Int] {
  override def setNone(preparedStatement: BoundStatement, parameterIndex: Int): Unit = {
    preparedStatement.setToNull(parameterIndex)
  }

  override def setAny(preparedStatement: BoundStatement, parameterIndex: Int, parameter: Any): Unit = {
    parameter match {
      case b: Boolean =>
        preparedStatement.setBool(parameterIndex, b)
      case b: java.lang.Boolean => b.booleanValue
      case b: ByteVector =>
        preparedStatement.setBytes(parameterIndex, b.toByteBuffer)
      case d: java.util.Date =>
        preparedStatement.setDate(parameterIndex, d)
      case d: java.math.BigDecimal =>
        preparedStatement.setDecimal(parameterIndex, d)
      case d: Double =>
        preparedStatement.setDouble(parameterIndex, d)
      case f: Float =>
        preparedStatement.setFloat(parameterIndex, f)
      case i: InetAddress =>
        preparedStatement.setInet(parameterIndex, i)
      case i: Int =>
        preparedStatement.setInt(parameterIndex, i)
      case l: java.util.List[_] =>
        preparedStatement.setList(parameterIndex, l)
      case l: Long =>
        preparedStatement.setLong(parameterIndex, l)
      case m: java.util.Map[_, _] =>
        preparedStatement.setMap(parameterIndex, m)
      case s: java.util.Set[_] =>
        preparedStatement.setSet(parameterIndex, s)
      case s: String =>
        preparedStatement.setString(parameterIndex, s)
      case u: java.util.UUID =>
        preparedStatement.setUUID(parameterIndex, u)
      case t: Token =>
        preparedStatement.setToken(parameterIndex, t)
      case t: TupleValue =>
        preparedStatement.setTupleValue(parameterIndex, t)
      case u: UDTValue =>
        preparedStatement.setUDTValue(parameterIndex, u)
      case b: java.math.BigInteger =>
        preparedStatement.setVarint(parameterIndex, b)
    }
  }
}
