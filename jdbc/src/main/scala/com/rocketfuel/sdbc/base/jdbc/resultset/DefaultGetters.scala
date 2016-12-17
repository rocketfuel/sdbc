package com.rocketfuel.sdbc.base.jdbc.resultset

import com.rocketfuel.sdbc.base.Getter
import com.rocketfuel.sdbc.base.jdbc._

trait DefaultGetters
  extends Getter
  with BooleanGetter
  with ByteGetter
  with BytesGetter
  with DateGetter
  with DoubleGetter
  with FloatGetter
  with IntGetter
  with JavaBigDecimalGetter
  with LongGetter
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
  self: DBMS =>

}
