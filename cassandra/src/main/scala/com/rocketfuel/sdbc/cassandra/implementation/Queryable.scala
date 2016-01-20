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
      def execute[Key](key: Key)(implicit queryable: Queryable[Key, _], session: Session): core.ResultSet = {
        queryable.query(key).io.execute()
      }

      def iterator[Key, Value](key: Key)(implicit queryable: Queryable[Key, Value], session: Session): Iterator[Value] = {
        queryable.query(key).io.iterator()
      }

      def option[Key, Value](key: Key)(implicit queryable: Queryable[Key, Value], session: Session): Option[Value] = {
        queryable.query(key).io.option()
      }
    }

    object future {
      def execute[Key](key: Key)(implicit queryable: Queryable[Key, _], session: Session, executionContext: ExecutionContext): Future[core.ResultSet] = {
        queryable.query(key).future.execute()
      }

      def iterator[Key, Value](key: Key)(implicit queryable: Queryable[Key, Value], session: Session, executionContext: ExecutionContext): Future[Iterator[Value]] = {
        queryable.query(key).future.iterator()
      }

      def option[Key, Value](key: Key)(implicit queryable: Queryable[Key, Value], session: Session, executionContext: ExecutionContext): Future[Option[Value]] = {
        queryable.query(key).future.option()
      }
    }

    object task {
      def execute[Key](key: Key)(implicit queryable: Queryable[Key, _], session: Session): Task[core.ResultSet] = {
        queryable.query(key).task.execute()
      }

      def iterator[Key, Value](key: Key)(implicit queryable: Queryable[Key, Value], session: Session): Task[Iterator[Value]] = {
        queryable.query(key).task.iterator()
      }

      def option[Key, Value](key: Key)(implicit queryable: Queryable[Key, Value], session: Session): Task[Option[Value]] = {
        queryable.query(key).task.option()
      }
    }

    def stream[Key, Value](key: Key)(implicit queryable: Queryable[Key, Value], session: Session): Process[Task, Value] = {
      queryable.query(key).stream()
    }

    def streams[Key, Value](cluster: core.Cluster)(implicit queryable: Queryable[Key, Value]): Channel[Task, Key, Process[Task, Value]] = {
      channel.lift[Task, Key, Process[Task, Value]] { key =>
        Task.delay {
          Process.await(toTask(cluster.connectAsync())) {implicit session =>
            stream[Key, Value](key).onComplete(Process.eval_(toTask(session.closeAsync())))
          }
        }
      }
    }

  }

}
