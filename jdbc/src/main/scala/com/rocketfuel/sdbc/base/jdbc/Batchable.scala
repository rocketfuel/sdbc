package com.rocketfuel.sdbc.base.jdbc

import fs2.util.Async

trait Batchable {
  self: DBMS with Connection =>

  trait Batchable[Key] {
    def batch(key: Key): Batch
  }

  object Batchable {
    def batch[Key](
      key: Key
    )(implicit batchable: Batchable[Key],
      connection: Connection
    ): IndexedSeq[Long] = {
      batchable.batch(key).batch()
    }

    def pipe[F[_], Key](
      key: Key
    )(implicit batchable: Batchable[Key],
      async: Async[F]
    ): Batch.Pipe[F] = {
      batchable.batch(key).pipe[F]
    }

    def sink[F[_], Key](
      key: Key
    )(implicit batchable: Batchable[Key],
      async: Async[F]
    ): Batch.Sink[F] = {
      batchable.batch(key).sink[F]
    }
  }

}
