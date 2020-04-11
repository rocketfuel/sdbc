package com.rocketfuel.sdbc.base.jdbc

import cats.effect.Async
import fs2.Stream

trait Selectable {
  self: DBMS with Connection =>

  trait Selectable[Key, Result] extends (Key => Select[Result]) {
    outer =>

    def mapWithKey[B](f: (Key, Result) => B): Selectable[Key, B] = {
      new Selectable[Key, B] {
        override def apply(v1: Key): Select[B] = {
          outer(v1).map(result => f(v1, result))
        }
      }
    }

    def map[B](f: Result => B): Selectable[Key, B] = {
      mapWithKey[B]((key, result) => f(result))
    }

    def comap[B](f: B => Key): Selectable[B, Result] = {
      new Selectable[B, Result] {
        override def apply(v1: B): Select[Result] = {
          outer(f(v1))
        }
      }
    }
  }


  object Selectable {
    def apply[Key, Value](implicit s: Selectable[Key, Value]): Selectable[Key, Value] = s

    implicit def create[Key, Value](f: Key => Select[Value]): Selectable[Key, Value] =
      new Selectable[Key, Value] {
        override def apply(key: Key): Select[Value] =
          f(key)
      }

    def iterator[Key, Result](
      key: Key
    )(implicit selectable: Selectable[Key, Result],
      connection: Connection
    ): CloseableIterator[Result] = {
      selectable(key).iterator()
    }

    def vector[Key, Result](
      key: Key
    )(implicit selectable: Selectable[Key, Result],
      connection: Connection
    ): Seq[Result] = {
      selectable(key).iterator().toVector
    }

    def option[Key, Result](
      key: Key
    )(implicit selectable: Selectable[Key, Result],
      connection: Connection
    ): Option[Result] = {
      selectable(key).option()
    }

    def one[Key, Result](
      key: Key
    )(implicit selectable: Selectable[Key, Result],
      connection: Connection
    ): Result = {
      selectable(key).one()
    }

    def stream[F[_], Key, Result](
      key: Key
    )(implicit selectable: Selectable[Key, Result],
      pool: Pool,
      async: Async[F]
    ): Stream[F, Result] = {
      selectable(key).stream[F]
    }

    def pipe[F[_], Key, Result](
      key: Key
    )(implicit selectable: Selectable[Key, Result],
      async: Async[F]
    ): Select.Pipe[F, Result] = {
      selectable(key).pipe[F]
    }

    def sink[F[_], Key](
      key: Key
    )(implicit selectable: Selectable[Key, _],
      async: Async[F]
    ): Ignore.Sink[F] = {
      selectable(key).sink[F]
    }

    trait Partable {
      implicit def selectablePartable[Key](implicit selectable: Selectable[Key, _]): Batch.Partable[Key] = {
        (key: Key) =>
          val query = selectable(key)
          Select.partable(query)
      }
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
