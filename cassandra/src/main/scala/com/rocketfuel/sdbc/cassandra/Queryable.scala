package com.rocketfuel.sdbc.cassandra

import com.datastax.driver.core.Cluster
import com.rocketfuel.sdbc.Cassandra._
import fs2.util.Async
import fs2.{Pipe, Stream}

trait Queryable[Key, Value] {
  queryable =>

  def query(key: Key): Query[Value]

  def withKeyspace(toKeyspace: Key => String): QueryableWithKeyspace[Key, Value] =
    new QueryableWithKeyspace[Key, Value] {
      override def keyspace(key: Key): String =
        toKeyspace(key)

      override def query(key: Key): Query[Value] =
        queryable.query(key)
    }
}

object Queryable {
  def apply[Key, Value](f: Key => Query[Value]): Queryable[Key, Value] =
    new Queryable[Key, Value] {
      override def query(key: Key): Query[Value] =
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

  def pipeWithKeyspace[F[_], Key, Value](implicit
    cluster: Cluster,
    queryable: Queryable[Key, Value],
    async: Async[F]
  ): Pipe[F, (Key, String), Stream[F, Value]] = {
    keyspaceAndKeys =>
      val keys = keyspaceAndKeys.map(_._1)
      val keyspaces = keyspaceAndKeys.map(_._2)
      val queries = keys.map(queryable.query)
      val sessions = keyspaces.through(StreamUtils.keyspaces)
      queries.zip(sessions).map {
        case (query, session) =>
          query.stream[F](session, implicitly[Async[F]])
      }
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
