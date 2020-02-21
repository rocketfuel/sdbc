package com.rocketfuel.sdbc.cassandra

import cats.effect.Async
import com.rocketfuel.sdbc.Cassandra._
import fs2.{Pipe, Stream}

trait Queryable[Key, Value] extends (Key => Query[Value]) {
  queryable =>

  def apply(key: Key): Query[Value]

  @deprecated("use apply", since = "3.0.0")
  def query(key: Key): Query[Value] = apply(key)

  def mapWithKey[B](f: (Key, Value) => B): Queryable[Key, B] = {
    new Queryable[Key, B] {
      override def apply(key: Key): Query[B] = {
        queryable(key).map(result => f(key, result))
      }
    }
  }

  def map[B](f: Value => B): Queryable[Key, B] = {
    mapWithKey((key, result) => f(result))
  }

  def comap[B](f: B => Key): Queryable[B, Value] = {
    new Queryable[B, Value] {
      override def apply(v1: B): Query[Value] = {
        queryable(f(v1))
      }
    }
  }
}

object Queryable {
  def apply[Key, Value](f: Key => Query[Value]): Queryable[Key, Value] =
    new Queryable[Key, Value] {
      override def apply(key: Key): Query[Value] =
        f(key)
    }

  def iterator[Key, Value](
    key: Key
  )(implicit queryable: Queryable[Key, Value],
    session: Session
  ): Iterator[Value] = {
    queryable.query(key).iterator()
  }

  def option[Key, Value](
    key: Key
  )(implicit queryable: Queryable[Key, Value],
    session: Session
  ): Option[Value] = {
    queryable.query(key).option()
  }

  def one[Key, Value](
    key: Key
  )(implicit queryable: Queryable[Key, Value],
    session: Session
  ): Value = {
    queryable.query(key).one()
  }

  def stream[F[_], Key, Value](
    key: Key
  )(implicit queryable: Queryable[Key, Value],
    session: Session,
    async: Async[F]
  ): Stream[F, Value] = {
    queryable.query(key).stream[F]
  }

  def pipe[F[_], Key, Value](implicit
    session: Session,
    queryable: Queryable[Key, Value],
    async: Async[F]
  ): Pipe[F, Key, Stream[F, Value]] = {
    keys =>
      keys.map(key => queryable.query(key).stream[F])
  }

  trait syntax {
    implicit class QueryableSyntax[Key](val key: Key) {
      def iterator[Value]()(implicit session: Session,  queryable: Queryable[Key, Value]): Iterator[Value] = {
        Queryable.iterator(key)
      }

      def option[Value]()(implicit session: Session,  queryable: Queryable[Key, Value]): Option[Value] = {
        Queryable.option(key)
      }

      def one[Value]()(implicit session: Session,  queryable: Queryable[Key, Value]): Value = {
        Queryable.one(key)
      }

      def stream[F[_], Value](implicit session: Session, async: Async[F],  queryable: Queryable[Key, Value]): Stream[F, Value] = {
        Queryable.stream(key)
      }
    }
  }

  object syntax extends syntax

}
