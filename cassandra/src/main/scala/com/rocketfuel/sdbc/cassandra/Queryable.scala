package com.rocketfuel.sdbc.cassandra

import com.datastax.driver.core
import com.rocketfuel.sdbc.base.StreamUtils
import fs2.util.Async
import fs2.{Pipe, Stream}

trait Queryable {
  self: Cassandra =>

  trait Queryable[Key, Value] {
    def query(key: Key): Query[Value]
  }

  object Queryable {

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

    def streams[F[_], Key, Value](
      implicit cluster: core.Cluster,
      queryable: Queryable[Key, Value],
      async: Async[F]
    ): Pipe[F, Key, Stream[F, Value]] = {
      val req = toAsync(cluster.connectAsync())
      def release(session: Session): F[Unit] = {
        async.map(toAsync(session.closeAsync()))(Function.const(()))
      }
      fs2.pipe.lift[F, Key, Stream[F, Value]] { key =>
        def use(session: Session) = {
          StreamUtils.fromIterator(queryable.query(key).iterator()(session))
        }
        fs2.Stream.bracket(req)(use, release)
      }
    }

    def streamsWithKeyspace[F[_], Key, Value](
      implicit cluster: core.Cluster,
      queryable: Queryable[Key, Value],
      async: Async[F]
    ): Pipe[F, (String, Key), Stream[F, Value]] = {
      def release(session: Session): F[Unit] = {
        async.map(toAsync(session.closeAsync()))(Function.const(()))
      }
      fs2.pipe.lift[F, (String, Key), Stream[F, Value]] {
        case (keyspace, key) =>
          val req = toAsync(cluster.connectAsync(keyspace))
          def use(session: Session) = {
            StreamUtils.fromIterator(queryable.query(key).iterator()(session))
          }
          fs2.Stream.bracket(req)(use, release)
      }
    }

  }

}
