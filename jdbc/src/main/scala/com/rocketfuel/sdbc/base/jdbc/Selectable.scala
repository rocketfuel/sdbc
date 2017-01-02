package com.rocketfuel.sdbc.base.jdbc

import fs2.Stream
import fs2.util.Async

trait Selectable {
  self: DBMS with Connection =>

  trait Selectable[Key, Result] extends Queryable[Select[Result], Key] {
    def select(key: Key): Select[Result]
  }

  object Selectable {
    def apply[Key, Value](implicit s: Selectable[Key, Value]): Selectable[Key, Value] = s

    def apply[Key, Value](f: Key => Select[Value]): Selectable[Key, Value] =
      new Selectable[Key, Value] {
        override def query(key: Key): Select[Value] = f(key)

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
      pool: Pool,
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

      implicit class SelectableSyntax[Key](key: Key) {
        def iterator[Result]()(implicit connection: Connection, selectable: Selectable[Key, Result]): CloseableIterator[Result] = {
          Selectable.iterator(key)
        }

        def vector[Result]()(implicit connection: Connection, selectable: Selectable[Key, Result]): Seq[Result] = {
          Selectable.vector(key)
        }

        def option[Result]()(implicit connection: Connection, selectable: Selectable[Key, Result]): Option[Result] = {
          Selectable.option(key)
        }

        def one[Result]()(implicit connection: Connection, selectable: Selectable[Key, Result]): Result = {
          Selectable.one(key)
        }

        def selectStream[F[_], Result](implicit pool: Pool, async: Async[F], selectable: Selectable[Key, Result]): Stream[F, Result] = {
          Selectable.stream(key)
        }

        def selectPipe[F[_], Result](implicit async: Async[F], selectable: Selectable[Key, Result]): Select.Pipe[F, Result] = {
          Selectable.pipe(key)
        }

        /**
          *
          * @tparam Result is only used to get the correct Selectable.
          */
        def selectSink[F[_], Result](implicit async: Async[F], selectable: Selectable[Key, Result]): Ignore.Sink[F] = {
          Selectable.sink(key)
        }
      }
    }

    object syntax extends syntax
  }

}
