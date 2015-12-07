package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.jdbc

trait DefaultSetters
  extends BooleanSetter
  with ByteSetter
  with BytesSetter
  with DateSetter
  with BigDecimalSetter
  with DoubleSetter
  with FloatSetter
  with IntSetter
  with LongSetter
  with ShortSetter
  with StringSetter
  with TimeSetter
  with TimestampSetter
  with ReaderSetter
  with InputStreamSetter
  with UUIDSetter
  with InstantSetter
  with LocalDateSetter
  with LocalTimeSetter
  with LocalDateTimeSetter {
  self: ParameterValue =>

  val toDefaultParameter: PartialFunction[Any, Any] =
    BooleanToParameter.toParameter orElse
      ByteToParameter.toParameter orElse
      BytesToParameter.toParameter orElse
      //Timestamp must come before Date, or else all Timestamps become Dates.
      TimestampToParameter.toParameter orElse
      //Time must come before Date, or else all Times become Dates.
      TimeToParameter.toParameter orElse
      DateToParameter.toParameter orElse
      BigDecimalToParameter.toParameter orElse
      DoubleToParameter.toParameter orElse
      FloatToParameter.toParameter orElse
      IntToParameter.toParameter orElse
      LongToParameter.toParameter orElse
      ShortToParameter.toParameter orElse
      StringToParameter.toParameter orElse
      ReaderToParameter.toParameter orElse
      InputStreamToParameter.toParameter orElse
      UUIDToParameter.toParameter orElse
      InstantToParameter.toParameter orElse
      LocalDateToParameter.toParameter orElse
      LocalTimeToParameter.toParameter orElse
      LocalDateTimeToParameter.toParameter

}
