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
            Task(queryable.query(key).iterator())
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
              Task(queryable.query(key).iterator())
            }
          }
      }
    }

  }

}
