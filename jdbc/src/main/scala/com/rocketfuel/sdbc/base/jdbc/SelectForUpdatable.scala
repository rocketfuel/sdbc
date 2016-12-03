package com.rocketfuel.sdbc.base.jdbc

import fs2.util.Async

trait SelectForUpdatable {
  self: DBMS with Connection =>

  trait SelectForUpdatable[Key] {
    def update(key: Key): SelectForUpdate
  }

  object SelectForUpdatable {

    def apply[Key](f: Key => SelectForUpdate): SelectForUpdatable[Key] =
      new SelectForUpdatable[Key] {
        override def update(key: Key): SelectForUpdate =
          f(key)
      }

    def update[Key](
      key: Key
    )(implicit selectable: SelectForUpdatable[Key],
      connection: Connection
    ): UpdatableRow.Summary = {
      selectable.update(key).update()
    }

    def pipe[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      updatable: SelectForUpdatable[Key]
    ): SelectForUpdate.Pipe[F] = {
      updatable.update(key).pipe[F]
    }

    def sink[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      updatable: SelectForUpdatable[Key]
    ): Ignore.Sink[F] = {
      updatable.update(key).sink[F]
    }
  }

}
