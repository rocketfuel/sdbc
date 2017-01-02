package com.rocketfuel.sdbc.base.jdbc

import fs2.util.Async

trait Updatable {
  self: DBMS with Connection =>

  trait Updatable[Key] extends Queryable[Update, Key] {
    def update(key: Key): Update
  }

  object Updatable {
    def apply[Key](implicit u: Updatable[Key]): Updatable[Key] = u

    def apply[Key](f: Key => Update): Updatable[Key] =
      new Updatable[Key] {
        override def query(key: Key): Update = f(key)

        override def update(key: Key): Update =
          f(key)
      }

    def update[Key](
      key: Key
    )(implicit updatable: Updatable[Key],
      connection: Connection
    ): Long = {
      updatable.update(key).update()
    }

    def pipe[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      updatable: Updatable[Key]
    ): Update.Pipe[F] = {
      updatable.update(key).pipe[F]
    }

    def sink[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      updatable: Updatable[Key]
    ): Ignore.Sink[F] = {
      updatable.update(key).sink[F]
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
