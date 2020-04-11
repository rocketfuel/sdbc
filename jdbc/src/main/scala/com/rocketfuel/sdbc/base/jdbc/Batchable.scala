package com.rocketfuel.sdbc.base.jdbc

import cats.effect.Async
import fs2.{Pipe, Stream}

trait Batchable {
  self: DBMS with Connection =>

  trait Batchable[Key] extends (Key => Batch)

  object Batchable {
    def apply[Key](implicit b: Batchable[Key]): Batchable[Key] = b

    implicit def create[Key](f: Key => Batch): Batchable[Key] =
      new Batchable[Key] {
        override def apply(key: Key): Batch =
          f(key)
      }

    implicit def ofPartable[Key](implicit partable: Batch.Partable[Key]): Batchable[Seq[Key]] = {
      new Batchable[Seq[Key]] {
        override def apply(keys: Seq[Key]): Batch = {
          Batch(keys.map(partable): _*)
        }
      }
    }

    def batch[Key](
      key: Key
    )(implicit batchable: Batchable[Key],
      connection: Connection
    ): Batch.Results = {
      batchable(key).batch()
    }

    def stream[F[_], Key](
      key: Key
    )(implicit batchable: Batchable[Key],
      pool: Pool,
      async: Async[F]
    ): Stream[F, Batch.Result] =
      batchable(key).stream()

    def pipe[
      F[_],
      Key
    ](implicit async: Async[F],
      p: Batch.Partable[Key],
      pool: Pool
    ): Pipe[F, Key, Batch.Result] =
      (keys: Stream[F, Key]) =>
        keys.map(p).through(Batch.pipe)

    trait syntax {
      implicit class BatchSyntax[Key](key: Key)(implicit batchable: Batchable[Key]) {
        def batch()(implicit connection: Connection): Batch.Results =
          Batchable.batch(key)

        def batchStream[
          F[_]
        ]()(implicit pool: Pool,
          async: Async[F]
        ): Stream[F, Batch.Result] =
          Batchable.stream(key)
      }

      implicit class BatchQuerySyntax[
        Key
      ](keys: Seq[Key]
      )(implicit p: Batch.Partable[Key]
      ) {
        def batches()(implicit connection: Connection): Batch.Results = {
          Batch.batch(keys.map(p): _*)
        }

        def streams[
          F[_]
        ](implicit async: Async[F],
          pool: Pool
        ): Stream[F, Batch.Result] = {
          Stream.emits[F, Key](keys).through(Batchable.pipe[F, Key])
        }
      }
    }

    object syntax extends syntax
  }

}
