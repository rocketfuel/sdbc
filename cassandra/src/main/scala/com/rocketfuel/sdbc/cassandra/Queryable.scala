package com.rocketfuel.sdbc.cassandra

import com.datastax.driver.core.Cluster
import com.rocketfuel.sdbc.base.Logger
import com.rocketfuel.sdbc.Cassandra._
import fs2.util.Async
import fs2.{Pipe, Stream}

trait Queryable[Key, Value] {
  def query(key: Key): Query[Value]
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

  def singleton[Key, Value](
    key: Key
  )(implicit queryable: Queryable[Key, Value],
    session: Session
  ): Value = {
    queryable.query(key).singleton()
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

  /**
    * Creates at most one session per keyspace.
    * Use this method when you want to manage the Cluster.
    */
  def pipeWithKeyspace[F[_], Key, Value](
    implicit cluster: Cluster,
    queryable: Queryable[Key, Value],
    async: Async[F]
  ): Pipe[F, (String, Key),  Stream[F, Value]] = {
    (s: Stream[F, (String, Key)]) =>
      s.zip(StreamUtils.sessionProviders).flatMap {
        case ((keyspace, key), sessionProvider) =>
          Stream.eval(sessionProvider(keyspace)).map {implicit session =>
            queryable.query(key).stream[F]
          }
      }
  }

  /**
    * Creates at most one session per keyspace.
    * The Cluster is created when the stream starts, and closed
    * when it completes.
    */
  def pipeWithKeyspace[F[_], Key, Value](
    initializer: Cluster.Initializer
  )(implicit queryable: Queryable[Key, Value],
    async: Async[F]
  ): Pipe[F, (String, Key), Stream[F, Value]] = {
    s =>
      StreamUtils.cluster(initializer) {implicit cluster =>
        s.through(pipeWithKeyspace)
      }
  }

}
