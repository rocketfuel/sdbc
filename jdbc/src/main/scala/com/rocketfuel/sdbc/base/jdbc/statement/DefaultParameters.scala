package com.rocketfuel.sdbc.base.jdbc.statement

trait DefaultParameters
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
  with InputStreamParameter
  with UUIDParameter {
  self: ParameterValue =>

}
