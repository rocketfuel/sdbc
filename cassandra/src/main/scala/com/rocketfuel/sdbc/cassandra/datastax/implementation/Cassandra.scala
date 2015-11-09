package com.rocketfuel.sdbc.cassandra.datastax.implementation

import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteBuffer

import com.datastax.driver.core.{TupleValue, UDTValue, Row => CRow}
import com.rocketfuel.sdbc.base.ParameterValueImplicits
import com.rocketfuel.sdbc.cassandra.datastax.implementation

private[sdbc] abstract class Cassandra
  extends RowMethods
  with ParameterValues
  with TupleParameterValues
  with TupleDataTypes
  with ParameterValueImplicits
  with SessionMethods
  with ExecutableMethods
  with SelectableMethods
  with StringContextMethods {

  type ParameterValue = implementation.ParameterValue

  type ParameterList = implementation.ParameterList

  type Session = implementation.Session

  type Cluster = implementation.Cluster

  type Executable[Key] = implementation.Executable[Key]

  type Selectable[Key, Value] = implementation.Selectable[Key, Value]

  implicit val ParameterGetter: RowGetter[ParameterValue] =
    new RowGetter[ParameterValue] {
      override def apply(row: CRow, ix: Index): Option[ParameterValue] = {

        Option(row.getObject(ix(row))).flatMap {
          case map: java.util.Map[_, _] =>
            //The drive returns NULL maps as empty maps rather than NULL.
            //This means that the result is ambiguous. We'll use None,
            //since that's what we'd expect when pattern matching.
            if (map.isEmpty) None
            else Some(JavaMapToParameter(map))
          case list: java.util.List[_] =>
            //Same semantics as for Map
            if (list.isEmpty) None
            else Some(JavaListToParameter(list))
          case set: java.util.Set[_] =>
            //Same semantics as for Map
            if (set.isEmpty) None
            else Some(JavaSetToParameter(set))
          case l: java.lang.Long =>
            Some(LongToParameter(l.longValue()))
          case b: ByteBuffer =>
            Some(ArrayByteToParameter(b.array()))
          case b: java.lang.Boolean =>
            Some(BooleanToParameter(b.booleanValue()))
          case d: java.math.BigDecimal =>
            Some(JavaBigDecimalToParameter(d))
          case d: java.lang.Double =>
            Some(DoubleToParameter(d.doubleValue()))
          case f: java.lang.Float =>
            Some(FloatToParameter(f.floatValue()))
          case i: InetAddress =>
            Some(InetAddressToParameter(i))
          case i: java.lang.Integer =>
            Some(IntToParameter(i.intValue()))
          case s: String =>
            Some(StringToParameter(s))
          case d: java.util.Date =>
            Some(DateToParameter(d))
          case u: java.util.UUID =>
            Some(UUIDToParameter(u))
          case b: BigInteger =>
            Some(BigIntegerToParameter(b))
          case u: UDTValue =>
            Some(UDTValueToParameter(u))
          case t: TupleValue =>
            Some(TupleValueToParameter(t))
        }

      }
    }

}
