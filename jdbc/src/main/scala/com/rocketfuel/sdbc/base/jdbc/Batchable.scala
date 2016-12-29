package com.rocketfuel.sdbc.base.jdbc

import fs2.util.Async

trait Batchable {
  self: DBMS with Connection =>

  trait Batchable[Key] {
    def batch(key: Key): Batch
  }

  object Batchable {
    def apply[Key](f: Key => Batch): Batchable[Key] =
      new Batchable[Key] {
        override def batch(key: Key): Batch =
          f(key)
      }

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

    trait syntax {
      implicit class BatchSyntax[Key](key: Key)(implicit batchable: Batchable[Key]) {
        def batch()(implicit connection: Connection): IndexedSeq[Long] = {
          Batchable.batch(key)
        }

        def batchPipe[F[_]](implicit async: Async[F]): Batch.Pipe[F] = {
          Batchable.pipe(key)
        }

        def batchSink[F[_]](implicit async: Async[F]): Batch.Sink[F] = {
          Batchable.sink(key)
        }
      }
    }

    object syntax extends syntax
  }

}
