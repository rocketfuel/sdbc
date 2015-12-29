package com.rocketfuel.sdbc.base.jdbc

trait DefaultUpdaters
  extends LongUpdater
  with IntUpdater
  with ShortUpdater
  with ByteUpdater
  with BytesUpdater
  with DoubleUpdater
  with FloatUpdater
  with BigDecimalUpdater
  with TimestampUpdater
  with DateUpdater
  with TimeUpdater
  with BooleanUpdater
  with StringUpdater
  with UUIDUpdater
  with InputStreamUpdater
  with ReaderUpdater
  with LocalDateTimeUpdater
  with InstantUpdater
  with LocalDateUpdater
  with LocalTimeUpdater {
  self: DBMS =>

}
