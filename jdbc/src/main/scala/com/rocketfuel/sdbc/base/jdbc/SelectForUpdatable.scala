package com.rocketfuel.sdbc.base.jdbc

import cats.effect.Async

trait SelectForUpdatable {
  self: DBMS with Connection =>

  trait SelectForUpdatable[Key] extends (Key => SelectForUpdate)

  object SelectForUpdatable {
    def apply[Key](implicit s: SelectForUpdatable[Key]): SelectForUpdatable[Key] = s

    implicit def create[Key](f: Key => SelectForUpdate): SelectForUpdatable[Key] =
      new SelectForUpdatable[Key] {
        override def apply(v1: Key): SelectForUpdate = f(v1)
      }

    def update[Key](
      key: Key
    )(implicit selectable: SelectForUpdatable[Key],
      connection: Connection
    ): UpdatableRow.Summary = {
      selectable(key).update()
    }

    def pipe[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      selectable: SelectForUpdatable[Key]
    ): SelectForUpdate.Pipe[F] = {
      selectable(key).pipe[F]
    }

    def sink[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      selectable: SelectForUpdatable[Key]
    ): Ignore.Sink[F] = {
      selectable(key).sink[F]
    }

    trait Partable {
      implicit def selectForUpdatablePartable[Key](implicit selectForUpdate: SelectForUpdatable[Key]): Batch.Partable[Key] = {
        (key: Key) =>
          val query = selectForUpdate(key)
          SelectForUpdate.partable(query)
      }
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
