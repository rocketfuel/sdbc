package com.rocketfuel.sdbc.base

/**
 * An update is a Batch of one statement. I.E. the iterator will have one element.
 * @tparam UnderlyingConnection
 */
abstract class Update[UnderlyingConnection] extends Batch[UnderlyingConnection] {
  self: Logging =>

  def update()(implicit connection: UnderlyingConnection): Long

  override def iterator()(implicit connection: UnderlyingConnection): Iterator[Long] = {
    Iterator(update())
  }

}
