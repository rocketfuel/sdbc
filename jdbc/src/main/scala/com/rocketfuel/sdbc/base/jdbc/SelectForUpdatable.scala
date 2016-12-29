package com.rocketfuel.sdbc.base.jdbc

import fs2.util.Async

trait SelectForUpdatable {
  self: DBMS with Connection =>

  trait SelectForUpdatable[Key] {
    def update(key: Key): SelectForUpdate

    def rowUpdater(key: Key): UpdatableRow => Unit
  }

  object SelectForUpdatable {

    def apply[Key](f: Key => SelectForUpdate, g: Key => UpdatableRow => Unit): SelectForUpdatable[Key] =
      new SelectForUpdatable[Key] {
        override def update(key: Key): SelectForUpdate =
          f(key)

        override def rowUpdater(key: Key): UpdatableRow => Unit =
          g(key)
      }

    def update[Key](
      key: Key
    )(implicit selectable: SelectForUpdatable[Key],
      connection: Connection
    ): UpdatableRow.Summary = {
      selectable.update(key).copy(rowUpdater = selectable.rowUpdater(key)).update()
    }

    def pipe[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      selectable: SelectForUpdatable[Key]
    ): SelectForUpdate.Pipe[F] = {
      selectable.update(key).copy(rowUpdater = selectable.rowUpdater(key)).pipe[F]
    }

    def sink[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      selectable: SelectForUpdatable[Key]
    ): Ignore.Sink[F] = {
      selectable.update(key).copy(rowUpdater = selectable.rowUpdater(key)).sink[F]
    }

    trait syntax {
      implicit class SelectForUpdatableSyntax[Key](key: Key)(implicit selectForUpdatable: SelectForUpdatable[Key]) {
        def selectForUpdateUpdate()(implicit connection: Connection): UpdatableRow.Summary = {
          SelectForUpdatable.update(key)
        }

        def selectForUpdatePipe[F[_]](implicit async: Async[F]): SelectForUpdate.Pipe[F] = {
          SelectForUpdatable.pipe(key)
        }

        def selectForUpdateSink[F[_]](implicit async: Async[F]): Ignore.Sink[F] = {
          SelectForUpdatable.sink(key)
        }
      }
    }

    object syntax extends syntax
  }

}
