package com.rocketfuel.sdbc.base.jdbc

trait Batchable {
  self: DBMS with Connection =>

  trait Batchable[Key] {
    def batch(key: Key): Batch
  }

  def batch[Key](
    key: Key
  )(implicit batchable: Batchable[Key],
    connection: Connection
  ): IndexedSeq[Long] = {
    batchable.batch(key).batch()
  }

}
