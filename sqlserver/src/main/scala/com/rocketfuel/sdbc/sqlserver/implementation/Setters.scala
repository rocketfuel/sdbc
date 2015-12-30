package com.rocketfuel.sdbc.sqlserver.implementation

import java.sql.{SQLXML, PreparedStatement}
import java.time.LocalTime
import java.util.UUID

import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.sqlserver.HierarchyId

import scala.xml.Node

private[sdbc] trait LocalTimeParameter {
  self: ParameterValue
    with StringParameter=>

  implicit object LocalTimeIsParameter extends PrimaryParameter[LocalTime] {
    override val toParameter: PartialFunction[Any, Any] = {
      case l: LocalTime => l.toString
    }
    override val setParameter: PartialFunction[Any, (Statement, Index) => Statement] =
      PartialFunction.empty
  }
}

private[sdbc] trait UUIDParameter {
  self: ParameterValue
    with StringParameter =>
  implicit object UUIDIsParameter extends PrimaryParameter[UUID] {
    override val toParameter: PartialFunction[Any, Any] = {
      case u: UUID => u.toString
    }
    override val setParameter: PartialFunction[Any, (Statement, Index) => Statement] =
      PartialFunction.empty
  }
}

private[sdbc] trait HierarchyIdParameter {
  self: ParameterValue
    with StringParameter =>
  implicit object HierarchyIdIsParameter extends PrimaryParameter[HierarchyId] {
    override val toParameter: PartialFunction[Any, Any] = {
      case h: HierarchyId => h.toString
    }
    override val setParameter: PartialFunction[Any, (Statement, Index) => Statement] =
      PartialFunction.empty
  }
}

private[sdbc] trait XMLParameter {
  self: ParameterValue
    with StringParameter =>

  implicit object XmlParameter
    extends PrimaryParameter[Node]
    with DerivedParameter[SQLXML] {

    override val toParameter: PartialFunction[Any, Any] = {
      case l: Node => l.toString()
      case s: SQLXML => s.getString
    }

    override val setParameter: PartialFunction[Any, (Statement, Index) => Statement] =
      PartialFunction.empty

  }

}

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
  with LocalTimeParameter
  with OffsetDateTimeAsStringParameter
  with UUIDParameter
  with HierarchyIdParameter
  with XMLParameter {
  self: ParameterValue =>

}
