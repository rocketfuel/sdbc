package com.wda.sdbc.scalaz

import java.util.concurrent.atomic.AtomicReference
import java.util.function.UnaryOperator

import com.datastax.driver.core.{BoundStatement, PreparedStatement, ResultSet, Row => CRow}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import com.wda.sdbc.cassandra.datastax._
import me.jeffshaw.scalaz.stream.IteratorConstructors._

import scalaz.concurrent.Task
import scalaz.stream._
import scalaz.{-\/, \/-}
import scala.collection.convert.wrapAsScala._

object datastax {

  implicit def ProcessToCassandraProcess(x: Process.type): DatastaxProcess.type = {
    DatastaxProcess
  }

  private [scalaz] def connect(cluster: Cluster, keyspace: Option[String] = None): Task[Session] = {
    Task.delay(keyspace.map(cluster.connect).getOrElse(cluster.connect()))
  }

  private [scalaz] def toTask[T](f: ListenableFuture[T]): Task[T] = {
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

  private [scalaz] def prepareAsync(
    query: ParameterizedQuery[_]
  )(implicit session: Session): Task[PreparedStatement] = {
    toTask(session.prepareAsync(query.queryText))
  }

  private [scalaz] def bind(
    query: ParameterizedQuery[_] with HasQueryOptions,
    statement: PreparedStatement
  ): BoundStatement = {
    val forBinding = statement.bind()

    for ((key, maybeValue) <- query.parameterValues) {
      val parameterIndices = query.parameterPositions(key)

      maybeValue match {
        case None =>
          for (parameterIndex <- parameterIndices) {
            forBinding.setToNull(parameterIndex - 1)
          }
        case Some(value) =>
          for (parameterIndex <- parameterIndices) {
            value.set(forBinding, parameterIndex - 1)
          }
      }
    }

    val queryOptions = query.queryOptions
    forBinding.setConsistencyLevel(queryOptions.consistencyLevel)
    forBinding.setSerialConsistencyLevel(queryOptions.serialConsistencyLevel)
    queryOptions.defaultTimestamp.map(forBinding.setDefaultTimestamp)
    forBinding.setFetchSize(queryOptions.fetchSize)
    forBinding.setIdempotent(queryOptions.idempotent)
    forBinding.setRetryPolicy(queryOptions.retryPolicy)

    if (queryOptions.tracing) {
      forBinding.enableTracing()
    } else {
      forBinding.disableTracing()
    }

    forBinding
  }

  private [scalaz] def runSelect[Value](
    select: Select[Value]
  )(implicit session: Session
  ): Process[Task, Value] = {
    Process.await(prepareAsync(select).map(p => bind(select, p))) { bound =>
      Process.await(runBoundStatement(bound))(_.map(select.converter))
    }
  }

  private def runBoundStatement(
    prepared: BoundStatement
  )(implicit session: Session
  ): Task[Process[Task, CRow]] = {
    toTask[ResultSet](session.executeAsync(prepared)).map { result =>
      Process.iterator(Task.delay(result.iterator()))
    }
  }

  private [scalaz] def runExecute(
    execute: Execute
  )(implicit session: Session
  ): Task[Unit] = {
    for {
      prepared <- prepareAsync(execute)
      bound = bind(execute, prepared)
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

  private [scalaz] def closeSession(session: Session): Task[Unit] = {
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
  private [scalaz] def forClusterWithKeyspaceAux[T, O](
  runner: T => Session => Task[O]
  )(cluster: Cluster): Channel[Task, (String, T), O] = {

    val sessionsRef = new AtomicReference(Map.empty[String, Session])

    /**
     * Get a Session for the keyspace, creating it if it does not exist.
     * @param keyspace
     * @return
     */
    def getSession(keyspace: String): Task[Session] = Task.delay {
      val sessions =
        sessionsRef.updateAndGet(
          new UnaryOperator[Map[String, Session]] {
            override def apply(t: Map[String, Session]): Map[String, Session] = {
              if (t.contains(keyspace)) t
              else {
                val session = cluster.connect(keyspace)
                t + (keyspace -> session)
              }
            }
          }
        )

      sessions(keyspace)
    }

    /**
     * Empty the sessions collection, and close all the sessions.
     */
    val closeSessions: Task[Unit] = {
      val getToClose = Task.delay[Map[String, Session]] {
        sessionsRef.getAndUpdate(
          new UnaryOperator[Map[String, Session]] {
            override def apply(t: Map[String, Session]): Map[String, Session] = {
              Map.empty
            }
          }
        )
      }

      for {
        toClose <- getToClose
        _ <- Task.gatherUnordered(toClose.map(kvp => closeSession(kvp._2)).toSeq)
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
