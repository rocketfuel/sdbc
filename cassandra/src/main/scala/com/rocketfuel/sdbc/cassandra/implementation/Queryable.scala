package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core
import scala.concurrent.{ExecutionContext, Future}
import scalaz.concurrent.Task
import scalaz.stream._

trait Queryable {
  self: Cassandra =>

  trait Queryable[Key, Value] {
    def query(key: Key): Query[Value]
  }

  object Queryable {

    object io {
      def iterator[Key, Value](
        key: Key
      )(implicit queryable: Queryable[Key, Value],
        session: Session
      ): Iterator[Value] = {
        queryable.query(key).io.iterator()
      }

      def option[Key, Value](
        key: Key
      )(implicit queryable: Queryable[Key, Value],
        session: Session
      ): Option[Value] = {
        queryable.query(key).io.option()
      }
    }

    object future {
      def iterator[Key, Value](
        key: Key
      )(implicit queryable: Queryable[Key, Value],
        session: Session,
        executionContext: ExecutionContext
      ): Future[Iterator[Value]] = {
        queryable.query(key).future.iterator()
      }

      def option[Key, Value](
        key: Key
      )(implicit queryable: Queryable[Key, Value],
        session: Session,
        executionContext: ExecutionContext
      ): Future[Option[Value]] = {
        queryable.query(key).future.option()
      }
    }

    object task {
      def iterator[Key, Value](
        key: Key
      )(implicit queryable: Queryable[Key, Value],
        session: Session
      ): Task[Iterator[Value]] = {
        queryable.query(key).task.iterator()
      }

      def option[Key, Value](
        key: Key
      )(implicit queryable: Queryable[Key, Value],
        session: Session
      ): Task[Option[Value]] = {
        queryable.query(key).task.option()
      }
    }

    def stream[Key, Value](
      key: Key
    )(implicit queryable: Queryable[Key, Value],
      session: Session
    ): Process[Task, Value] = {
      queryable.query(key).stream()
    }

    def streams[Key, Value](
      implicit cluster: core.Cluster,
      queryable: Queryable[Key, Value]
    ): Channel[Task, Key, Process[Task, Value]] = {
      val req = toTask(cluster.connectAsync())
      def release(session: Session): Task[Unit] = {
        toTask(session.closeAsync()).map(Function.const(()))
      }
      channel.lift[Task, Key, Process[Task, Value]] { key =>
        Task.delay {
          scalaz.stream.io.iteratorR[Session, Value](req)(release) {implicit session =>
            queryable.query(key).task.iterator()
          }
        }
      }
    }

    def streamsWithKeyspace[Key, Value](
      implicit cluster: core.Cluster,
      queryable: Queryable[Key, Value]
    ): Channel[Task, (String, Key), Process[Task, Value]] = {
      def release(session: Session): Task[Unit] = {
        toTask(session.closeAsync()).map(Function.const(()))
      }
      channel.lift[Task, (String, Key), Process[Task, Value]] {
        case (keyspace, key) =>
          val req = toTask(cluster.connectAsync(keyspace))
          Task.delay {
            scalaz.stream.io.iteratorR[Session, Value](req)(release) {implicit session =>
              queryable.query(key).task.iterator()
            }
          }
      }
    }

  }

}
