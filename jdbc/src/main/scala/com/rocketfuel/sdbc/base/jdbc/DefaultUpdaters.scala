package com.rocketfuel.sdbc.base.jdbc

trait DefaultUpdaters {
  self: Updater
    with UpdatableRow
    with ParameterValue
    with MutableRow =>

  trait DefaultUpdaters
    extends AnyRefUpdater
    with LongUpdater
    with IntUpdater
    with ShortUpdater
    with ByteUpdater
    with BytesUpdater
    with DoubleUpdater
    with FloatUpdater
    with JavaBigDecimalUpdater
    with ScalaBigDecimalUpdater
    with TimestampUpdater
    with DateUpdater
    with TimeUpdater
    with BooleanUpdater
    with StringUpdater
    with UUIDUpdater
    with InputStreamUpdater
    with UpdateReader

  trait Java8DefaultUpdaters
    extends DefaultUpdaters
    with LocalDateTimeUpdater
    with InstantUpdater
    with LocalDateUpdater
    with LocalTimeUpdater

}