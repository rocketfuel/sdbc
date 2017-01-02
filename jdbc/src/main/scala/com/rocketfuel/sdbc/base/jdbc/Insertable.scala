package com.rocketfuel.sdbc.base.jdbc

import fs2.util.Async

trait Insertable {
  self: DBMS with Connection =>

  trait Insertable[Key] {
    def insert(key: Key): Insert
  }

  object Insertable {
    def apply[Key](f: Key => Insert): Insertable[Key] =
      new Insertable[Key] {
        override def insert(key: Key): Insert =
          f(key)
      }

    def insert[Key](
      key: Key
    )(implicit Insertable: Insertable[Key],
      connection: Connection
    ): Long = {
      Insertable.insert(key).insert()
    }

    def pipe[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      Insertable: Insertable[Key]
    ): Insert.Pipe[F] = {
      Insertable.insert(key).pipe[F]
    }

    def sink[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      Insertable: Insertable[Key]
    ): Ignore.Sink[F] = {
      Insertable.insert(key).sink[F]
    }

    trait syntax {
      implicit class InsertableSyntax[Key](key: Key)(implicit insertable: Insertable[Key]) {
        def insert()(implicit connection: Connection): Long = {
          Insertable.insert(key)
        }

        def insertPipe[F[_]](implicit async: Async[F]): Insert.Pipe[F] = {
          Insertable.pipe(key)
        }

        def insertSink[F[_]](implicit async: Async[F]): Ignore.Sink[F] = {
          Insertable.sink(key)
        }
      }
    }

    object syntax extends syntax
  }

}
