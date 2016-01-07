package com.rocketfuel.sdbc.sqlserver.implementation

import java.util.UUID
import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.sqlserver.HierarchyId
import scala.xml.Node

//We have to use a special UUID getter, so we can't use the default setters.
private[sdbc] trait Setters
  extends BooleanParameter
  with ByteParameter
  with BytesParameter
  with DateParameter
  with BigDecimalParameter
  with DoubleParameter
  with FloatParameter
  with IntParameter
  with LongParameter
  with ShortParameter
  with StringParameter
  with TimeParameter
  with TimestampParameter
  with ReaderParameter
  with InputStreamParameter
  with OffsetDateTimeAsStringParameter {
  self: ParameterValue =>

  implicit val HierarchyIdParameter: Parameter[HierarchyId] = {
    (id: HierarchyId) =>
      val asString = id.toString
      StringParameter.set(asString)
  }

  implicit val UUIDParameter: Parameter[UUID] = {
    (uuid: UUID) =>
      val asString = uuid.toString
      StringParameter.set(asString)
  }

  implicit val XmlParameter: Parameter[Node] = {
    (node: Node) =>
      val asString = node.toString
      StringParameter.set(asString)
  }

}
