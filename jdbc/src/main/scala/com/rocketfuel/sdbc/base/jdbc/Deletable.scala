package com.rocketfuel.sdbc.base.jdbc

import fs2.util.Async

trait Deletable {
  self: DBMS with Connection =>

  trait Deletable[Key] extends (Key => Delete)

  object Deletable {
    def apply[Key](implicit d: Deletable[Key]): Deletable[Key] = d

    implicit def create[Key](f: Key => Delete): Deletable[Key] =
      new Deletable[Key] {
        override def apply(key: Key): Delete =
          f(key)
      }

    def delete[Key](
      key: Key
    )(implicit deletable: Deletable[Key],
      connection: Connection
    ): Long = {
      deletable(key).delete()
    }

    def pipe[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      deletable: Deletable[Key]
    ): Delete.Pipe[F] = {
      deletable(key).pipe[F]
    }

    def sink[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      deletable: Deletable[Key]
    ): Ignore.Sink[F] = {
      deletable(key).sink[F]
    }

    trait Partable {
      implicit def deletablePartable[Key](implicit deletable: Deletable[Key]): Batch.Partable[Key] = {
        (key: Key) =>
          val query = deletable(key)
          Delete.partable(query)
      }
    }

    trait syntax {
      implicit class DeletableSyntax[Key](key: Key)(implicit deletable: Deletable[Key]) {
        def delete()(implicit connection: Connection): Long = {
          Deletable.delete(key)
        }

        def deletePipe[F[_]](implicit async: Async[F]): Delete.Pipe[F] = {
          Deletable.pipe(key)
        }

        def deleteSink[F[_]](implicit async: Async[F]): Ignore.Sink[F] = {
          Deletable.sink(key)
        }
      }
    }

    object syntax extends syntax
  }

}
