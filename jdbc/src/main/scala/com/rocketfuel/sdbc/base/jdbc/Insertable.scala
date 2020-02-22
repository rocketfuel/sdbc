package com.rocketfuel.sdbc.base.jdbc

import cats.effect.Async

trait Insertable {
  self: DBMS with Connection =>

  trait Insertable[Key] extends (Key => Insert)

  object Insertable {
    def apply[Key](implicit i: Insertable[Key]): Insertable[Key] = i

    implicit def create[Key](f: Key => Insert): Insertable[Key] =
      new Insertable[Key] {
        override def apply(key: Key): Insert =
          f(key)
      }

    def insert[Key](
      key: Key
    )(implicit insertable: Insertable[Key],
      connection: Connection
    ): Long = {
      insertable(key).insert()
    }

    def pipe[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      insertable: Insertable[Key]
    ): Insert.Pipe[F] = {
      insertable(key).pipe[F]
    }

    def sink[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      insertable: Insertable[Key]
    ): Ignore.Sink[F] = {
      insertable(key).sink[F]
    }

    trait Partable {
      implicit def insertablePartable[Key](implicit insertable: Insertable[Key]): Batch.Partable[Key] = {
        (key: Key) =>
          val query = insertable(key)
          Insert.partable(query)
      }
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
