package com.rocketfuel.sdbc.sqlserver

import com.rocketfuel.sdbc.base.jdbc.statement._
import java.time._
import java.util.UUID
import scala.xml.Node

//We have to use a special UUID getter, so we can't use the default setters.
trait Setters
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
  with ReaderParameter
  with InputStreamParameter {
  self: SqlServer =>

  implicit val OffsetDateTimeParameter =
    new DerivedParameter[OffsetDateTime] {
      override type B = String
      override val conversion: OffsetDateTime => B = offsetDateTimeFormatter.format
      override val baseParameter: Parameter[B] = StringParameter
    }

  override implicit val InstantParameter: DerivedParameter[Instant] =
    new DerivedParameter[Instant] {
      override type B = String
      override val conversion: Instant => B = {(i: Instant) =>
        val value = offsetDateTimeFormatter.format(i.atOffset(ZoneOffset.UTC))
        value
      }
      override val baseParameter: Parameter[B] = StringParameter
    }

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
