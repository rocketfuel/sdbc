package com.rocketfuel.sdbc.base.jdbc

import fs2.Stream
import fs2.util.Async

trait Selectable {
  self: DBMS with Connection =>

  trait Selectable[Key, Result] {
    def select(key: Key): Select[Result]
  }

  object Selectable {
    def apply[Key, Value](f: Key => Select[Value]): Selectable[Key, Value] =
      new Selectable[Key, Value] {
        override def select(key: Key): Select[Value] =
          f(key)
      }

    def iterator[Key, Result](
      key: Key
    )(implicit selectable: Selectable[Key, Result],
      connection: Connection
    ): CloseableIterator[Result] = {
      selectable.select(key).iterator()
    }

    def vector[Key, Result](
      key: Key
    )(implicit selectable: Selectable[Key, Result],
      connection: Connection
    ): Seq[Result] = {
      selectable.select(key).iterator().toVector
    }

    def option[Key, Result](
      key: Key
    )(implicit selectable: Selectable[Key, Result],
      connection: Connection
    ): Option[Result] = {
      selectable.select(key).option()
    }

    def one[Key, Result](
      key: Key
    )(implicit selectable: Selectable[Key, Result],
      connection: Connection
    ): Result = {
      selectable.select(key).one()
    }

    def stream[F[_], Key, Result](
      key: Key
    )(implicit selectable: Selectable[Key, Result],
      connection: Connection,
      async: Async[F]
    ): Stream[F, Result] = {
      selectable.select(key).stream[F]
    }

    def pipe[F[_], Key, Result](
      key: Key
    )(implicit selectable: Selectable[Key, Result],
      async: Async[F]
    ): Select.Pipe[F, Result] = {
      selectable.select(key).pipe[F]
    }

    def sink[F[_], Key](
      key: Key
    )(implicit selectable: Selectable[Key, _],
      async: Async[F]
    ): Ignore.Sink[F] = {
      selectable.select(key).sink[F]
    }

    trait syntax {

      implicit class SelectableSyntax[Key, Result](key: Key)(implicit selectable: Selectable[Key, Result]) {
        def iterator()(implicit connection: Connection): CloseableIterator[Result] = {
          Selectable.iterator(key)
        }

        def vector()(implicit connection: Connection): Seq[Result] = {
          Selectable.vector(key)
        }

        def option()(implicit connection: Connection): Option[Result] = {
          Selectable.option(key)
        }

        def one()(implicit connection: Connection): Result = {
          Selectable.one(key)
        }

        def stream[F[_]](implicit connection: Connection, async: Async[F]): Stream[F, Result] = {
          Selectable.stream(key)
        }

        def selectPipe[F[_]](implicit async: Async[F]): Select.Pipe[F, Result] = {
          Selectable.pipe(key)
        }

        def selectSink[F[_]](implicit async: Async[F]): Ignore.Sink[F] = {
          Selectable.sink(key)
        }
      }
    }

    object syntax extends syntax
  }

}
