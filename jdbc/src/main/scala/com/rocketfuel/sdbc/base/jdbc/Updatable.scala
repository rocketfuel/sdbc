package com.rocketfuel.sdbc.base.jdbc

import fs2.util.Async

trait Updatable {
  self: DBMS with Connection =>

  trait Updatable[Key] extends (Key => Update)

  object Updatable {
    def apply[Key](implicit u: Updatable[Key]): Updatable[Key] = u

    implicit def create[Key](f: Key => Update): Updatable[Key] =
      new Updatable[Key] {
        override def apply(key: Key): Update = f(key)
      }

    def update[Key](
      key: Key
    )(implicit updatable: Updatable[Key],
      connection: Connection
    ): Long = {
      updatable(key).update()
    }

    def pipe[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      updatable: Updatable[Key]
    ): Update.Pipe[F] = {
      updatable(key).pipe[F]
    }

    def sink[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      updatable: Updatable[Key]
    ): Ignore.Sink[F] = {
      updatable(key).sink[F]
    }

    trait Partable {
      implicit def updatablePartable[Key](implicit updatable: Updatable[Key]): Batch.Partable[Key] = {
        (key: Key) =>
          val query = updatable(key)
          Update.partable(query)
      }
    }

    trait syntax {
      implicit class UpdatableSyntax[Key](key: Key)(implicit updatable: Updatable[Key]) {
        def update()(implicit connection: Connection): Long = {
          Updatable.update(key)
        }

        def updatePipe[F[_]](implicit async: Async[F]): Update.Pipe[F] = {
          Updatable.pipe(key)
        }

        def updateSink[F[_]](implicit async: Async[F]): Ignore.Sink[F] = {
          Updatable.sink(key)
        }
      }
    }

    object syntax extends syntax
  }

}
