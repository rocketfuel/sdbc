package com.rocketfuel.sdbc.base

import com.rocketfuel.sdbc.base

trait Batchable {

  type Connection

  type Batch <: base.Batch[Connection]

  trait Batchable[Key] {
    def batch(key: Key): Batch
  }

  def batchIterator[Key](
    key: Key
  )(implicit batchable: Batchable[Key],
    connection: Connection
  ): Iterator[Long] = {
    batchable.batch(key).iterator()
  }

}
