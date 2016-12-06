package com.rocketfuel.sdbc.cassandra

import com.datastax.driver.core.Cluster
import com.google.common.util.concurrent._
import com.rocketfuel.sdbc.Cassandra.Session
import com.rocketfuel.sdbc.base.Logger
import fs2.{Pipe, Stream}
import fs2.util.{Async, NonFatal}
import fs2.util.syntax._
import java.util.concurrent._
import java.util.function.BiFunction

object StreamUtils extends Logger {

  /**
    * Create a stream from a managed Cluster.
    */
  def cluster[F[_], O](
    initializer: Cluster.Initializer
  )(use: Cluster => Stream[F, O]
  )(implicit async: Async[F]
  ): Stream[F, O] = {
    val req = async.delay {
      val c = Cluster.buildFrom(initializer)
      c.init()
    }
    def release(cluster: Cluster): F[Unit] = {
      toAsync(cluster.closeAsync()).map(Function.const(()))
    }
    Stream.bracket(req)(use, release)
  }

  /**
    * Create a stream from a managed Session.
    */
  def session[F[_], O](
    use: Session => Stream[F, O]
  )(implicit cluster: Cluster,
    async: Async[F]
  ): Stream[F, O] = {
    val req = toAsync(cluster.connectAsync())
    def release(session: Session): F[Unit] = {
      async.map(toAsync(session.closeAsync()))(Function.const(()))
    }
    Stream.bracket(req)(use, release)
  }

  /**
    * Create a session, registering it with the async callback
    * when it is created, or when creation fails.
    */
  private def createSession(
    keyspace: String,
    register: (scala.Either[scala.Throwable, Session]) => Unit
  )(implicit cluster: Cluster
  ): Session = {
    log.info("creating session for keyspace {}", keyspace: Any)
    val f = cluster.connectAsync(keyspace)
    val callback = new FutureCallback[Session] {
      override def onFailure(t: Throwable): Unit = {
        log.warn(s"session creation for keyspace $keyspace failed", t)
        register(Left(t))
      }
      override def onSuccess(result: Session): Unit = {
        log.debug(s"session creation for keyspace {} succeeded", keyspace: Any)
        register(Right(result))
      }
    }
    Futures.addCallback(f, callback, executor)
    f.get()
  }

  /**
    * Maps a stream of keyspaces into a stream of Sessions for those keyspaces.
    * At most one Session is created per keyspace.
    *
    * It uses a map to store sessions. Create one Session for each keyspace on demand, and keep Sessions open for
    * further queries.
    */
  def keyspaces[F[_]](implicit async: Async[F], cluster: Cluster): Pipe[F, String, Session] = {
    (keyspaces: Stream[F, String]) =>
    Stream.bracket(
      r = async.delay(new ConcurrentHashMap[String, Session]())
    )(use = { (sessions: ConcurrentHashMap[String, Session]) =>
      def lookup(keyspace: String): F[Session] = {
        async.async[Session](register =>
          async.delay(
            /*
          When creating the session for this keyspace, we don't actually use the return value of compute(),
          which is synchronous. Instead, we use a callback on connectAsync().

          Any additional requests for this keyspace's session will use the return value of compute().
           */
            sessions.compute(
              keyspace,
              new BiFunction[String, Session, Session] {
                override def apply(
                  t: String,
                  u: Session
                ): Session = {
                  if (u == null) {
                    createSession(t, register)
                  } else {
                    try {
                      register(Right(u))
                      u
                    } catch {
                      case NonFatal(e) =>
                        log.debug(s"retrying session creation for keyspace $t", e)
                        createSession(t, register)
                    }
                  }
                }
              }
            )
          )
        )
      }
        for {
          keyspace <- keyspaces
          session <- Stream.eval(lookup(keyspace))
        } yield session
    },
      release = { sessionsPar =>
        import scala.collection.JavaConverters._
        log.debug("closing sessions")
        val sessions = sessionsPar.values().asScala.toVector
        val closers =
          for (session <- sessions) yield {
            toAsync(session.closeAsync())
          }
        closers.sequence.map(Function.const(()))
      }
    )
  }

}
