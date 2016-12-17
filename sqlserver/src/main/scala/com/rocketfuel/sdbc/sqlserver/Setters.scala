package com.rocketfuel.sdbc.sqlserver

import com.rocketfuel.sdbc.base.jdbc.statement._
import java.time._
import java.util.UUID
import scala.xml.Elem

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

  implicit val OffsetDateTimeParameter: Parameter[OffsetDateTime] =
    DerivedParameter.converted[OffsetDateTime, String](offsetDateTimeFormatter.format)

  override implicit val InstantParameter: Parameter[Instant] =
    DerivedParameter.converted[Instant, String](i => offsetDateTimeFormatter.format(i.atOffset(ZoneOffset.UTC)))

  implicit val HierarchyIdParameter: Parameter[HierarchyId] =
    DerivedParameter.toString[HierarchyId]

  implicit val UUIDParameter: Parameter[UUID] = {
    (uuid: UUID) =>
      val asString = uuid.toString
      StringParameter.set(asString)
  }

  implicit val XmlElemParameter: Parameter[Elem] =
    DerivedParameter.toString[Elem]

}
