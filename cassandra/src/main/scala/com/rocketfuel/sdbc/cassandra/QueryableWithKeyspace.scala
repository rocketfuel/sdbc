package com.rocketfuel.sdbc.cassandra

import com.datastax.driver.core.Cluster
import fs2.{Pipe, Stream}
import fs2.util.Async

trait QueryableWithKeyspace[Key, Value] extends Queryable[Key, Value] {
  def keyspace(key: Key): String
}

object QueryableWithKeyspace {
  def apply[Key, Value](k: Key => String, q: Key => Query[Value]): QueryableWithKeyspace[Key, Value] =
    new QueryableWithKeyspace[Key, Value] {
      override def query(key: Key): Query[Value] = q(key)
      override def keyspace(key: Key): String = k(key)
    }

  /**
    * Creates at most one session per keyspace.
    * Use this method with [[StreamUtils.cluster]].
    */
  def pipe[F[_], Key, Value](implicit
    cluster: Cluster,
    queryable: QueryableWithKeyspace[Key, Value],
    async: Async[F]
  ): Pipe[F, Key,  Stream[F, Value]] = {
    (keys: Stream[F, Key]) =>
      val keysWithKeyspaces =
        for {
          key <- keys
        } yield (key, queryable.keyspace(key))
      keysWithKeyspaces.through(Queryable.pipeWithKeyspace)
  }
}
