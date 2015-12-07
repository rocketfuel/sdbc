package com.rocketfuel.sdbc.base.jdbc

trait DefaultGetters
  extends Getter
  with BooleanGetter
  with ByteGetter
  with BytesGetter
  with DateGetter
  with DoubleGetter
  with FloatGetter
  with InputStreamGetter
  with IntGetter
  with JavaBigDecimalGetter
  with LongGetter
  with ReaderGetter
  with ScalaBigDecimalGetter
  with ShortGetter
  with StringGetter
  with TimeGetter
  with TimestampGetter
  with UUIDGetter
  with InstantGetter
  with LocalDateGetter
  with LocalDateTimeGetter
  with LocalTimeGetter {
  self: Row
    with MutableRow
    with Index
    with ResultSetImplicits =>

}
