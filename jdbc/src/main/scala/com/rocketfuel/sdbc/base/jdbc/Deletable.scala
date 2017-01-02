package com.rocketfuel.sdbc.base.jdbc

import fs2.util.Async

trait Deletable {
  self: DBMS with Connection =>

  trait Deletable[Key] {
    def delete(key: Key): Delete
  }

  object Deletable {
    def apply[Key](f: Key => Delete): Deletable[Key] =
      new Deletable[Key] {
        override def delete(key: Key): Delete =
          f(key)
      }

    def delete[Key](
      key: Key
    )(implicit Deletable: Deletable[Key],
      connection: Connection
    ): Long = {
      Deletable.delete(key).delete()
    }

    def pipe[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      Deletable: Deletable[Key]
    ): Delete.Pipe[F] = {
      Deletable.delete(key).pipe[F]
    }

    def sink[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      Deletable: Deletable[Key]
    ): Ignore.Sink[F] = {
      Deletable.delete(key).sink[F]
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
