package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core.{BoundStatement, PreparedStatement, ResultSet, Row => CRow}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import com.rocketfuel.sdbc.cassandra._
import scala.collection.concurrent.TrieMap
import scalaz.concurrent.Task
import scalaz.stream._
import scalaz.{-\/, \/-}
import scala.collection.convert.wrapAsScala._

private[implementation] object ProcessUtils {

  def connect(cluster: Cluster, keyspace: Option[String] = None): Task[Session] = {
    Task.delay(keyspace.map(cluster.connect).getOrElse(cluster.connect()))
  }

  /**
    * Convert a Google future to a Task.
    * @param f
    * @tparam T
    * @return
    */
  def toTask[T](f: ListenableFuture[T]): Task[T] = {
    Task.async[T] { callback =>
      val googleCallback = new FutureCallback[T] {
        override def onFailure(t: Throwable): Unit = {
          callback(-\/(t))
        }

        override def onSuccess(result: T): Unit = {
          callback(\/-(result))
        }
      }

      Futures.addCallback(f, googleCallback)
    }
  }

  def prepareAsync(
    query: com.rocketfuel.sdbc.cassandra.implementation.ParameterizedQuery[_]
  )(implicit session: Session
  ): Task[PreparedStatement] = {
    toTask(session.prepareAsync(query.queryText))
  }

  def runSelect[Value](
    select: Select[Value]
  )(implicit session: Session
  ): Process[Task, Value] = {
    Process.await(prepareAsync(select).map(p => implementation.bind(select, select.queryOptions, p))) { bound =>
      Process.await(runBoundStatement(bound))(_.map(select.converter))
    }
  }

  private def runBoundStatement(
    prepared: BoundStatement
  )(implicit session: Session
  ): Task[Process[Task, CRow]] = {
    toTask[ResultSet](session.executeAsync(prepared)).map { result =>
      io.iterator(Task.delay(result.iterator()))
    }
  }

  def runExecute(
    execute: Execute
  )(implicit session: Session
  ): Task[Unit] = {
    for {
      prepared <- prepareAsync(execute)
      bound = implementation.bind(execute, execute.queryOptions, prepared)
      _ <- ignoreBoundStatement(bound)
    } yield ()
  }

  private def ignoreBoundStatement(
    prepared: BoundStatement
  )(implicit session: Session
  ): Task[Unit] = {
    val rsFuture = session.executeAsync(prepared)
    toTask(rsFuture).map(Function.const(()))
  }

  def closeSession(session: Session): Task[Unit] = {
    val f = session.closeAsync()
    toTask(f).map(Function.const(()))
  }

  /**
   * Run statements, but open only one session per keyspace.
   * Sessions are created when they are first required.
   * @param runner
   * @param cluster
   * @tparam T
   * @tparam O
   * @return
   */
  def forClusterWithKeyspaceAux[T, O](
    runner: T => Session => Task[O]
  )(cluster: Cluster
  ): Channel[Task, (String, T), O] = {

    val sessions = TrieMap.empty[String, Session]

    /**
     * Get a Session for the keyspace, creating it if it does not exist.
     * @param keyspace
     * @return
     */
    def getSession(keyspace: String): Task[Session] = Task.delay {
      sessions.getOrElseUpdate(keyspace, cluster.connect(keyspace))
    }

    /**
     * Empty the sessions collection, and close all the sessions.
     */
    val closeSessions: Task[Unit] = {
      val getToClose = Task.delay[Iterable[Session]] {
        val toClose = sessions.readOnlySnapshot().values
        sessions.clear()
        toClose
      }

      for {
        toClose <- getToClose
        _ <- Task.gatherUnordered(toClose.toSeq.map(closeSession))
      } yield ()
    }

    channel.lift[Task, (String, T), O]{ case (keyspace, thing) =>
      for {
        session <- getSession(keyspace)
        result <- runner(thing)(session)
      } yield result
    }.onComplete(Process.eval_(closeSessions))
  }

}
