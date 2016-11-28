package com.rocketfuel.sdbc.base.jdbc

import fs2.util.Async

trait Updatable {
  self: DBMS with Connection =>

  trait Updatable[Key] {
    def update(key: Key): Update
  }

  object Updatable {
    def apply[Key](f: Key => Update): Updatable[Key] =
      new Updatable[Key] {
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
      updatable: Updatable[Key],
      connection: Connection
    ): Update.Pipe[F] = {
      updatable.update(key).pipe[F]
    }

    def sink[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      updatable: Updatable[Key],
      connection: Connection
    ): Ignore.Sink[F] = {
      updatable.update(key).sink[F]
    }
  }

}
