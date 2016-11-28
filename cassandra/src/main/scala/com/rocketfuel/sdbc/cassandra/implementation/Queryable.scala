package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import com.rocketfuel.sdbc.base.Logger
import fs2.util.{Async, NonFatal}
import fs2.util.syntax._
import fs2.{Pipe, Stream}
import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiFunction

trait Queryable {
  self: Cassandra =>

  trait Queryable[Key, Value] {
    def query(key: Key): Query[Value]
  }

  object Queryable
    extends Logger {
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

    def pipe[F[_], Key, Value](
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
          queryable.query(key).stream[F](session, async)
        }
        fs2.Stream.bracket(req)(use, release)
      }
    }

    private def createSession(
      keyspace: String,
      register: (scala.Either[scala.Throwable, Session]) => Unit
    )(implicit cluster: Cluster
    ): ListenableFuture[Session] = {
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
      Futures.addCallback(f, callback)
      f
    }

    /**
      * Use a map to store sessions. Create Sessions to keyspaces on demand, and keep them open for
      * further queries.
      */
    private def sessionProviders[F[_]](implicit async: Async[F], cluster: Cluster): Stream[F, String => F[Session]] = {
      Stream.bracket[F, ConcurrentHashMap[String, ListenableFuture[Session]], String => F[Session]](
        r = async.delay(new ConcurrentHashMap[String, ListenableFuture[Session]]())
      )(use = { (sessions: ConcurrentHashMap[String, ListenableFuture[Session]]) =>
        def lookup(keyspace: String): F[Session] = {
          async.async[Session](register =>
            async.pure(
              /*
              We don't actually use the return value of compute(), which is synchronous.
              Instead, we use a callback on connectAsync().
               */
              sessions.compute(
                keyspace,
                new BiFunction[String, ListenableFuture[Session], ListenableFuture[Session]] {
                  override def apply(
                    t: String,
                    u: ListenableFuture[Session]
                  ): ListenableFuture[Session] = {
                    if (u == null) {
                      createSession(t, register)
                    } else {
                      try {
                        register(
                          Right {
                            val session = u.get()
                            log.debug("found open session for keyspace {}", t: Any)
                            session
                          }
                        )
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
        Stream(lookup _).repeat
      },
        release = { sessionsPar =>
          import scala.collection.convert.wrapAsScala._
          log.debug("closing sessions")
          val sessions = sessionsPar.values().toVector
          val closers =
            for {
              session <- sessions
            } yield {
              toAsync(session.get().closeAsync())
            }
          closers.sequence.map(Function.const(()))
        }
      )
    }

    /**
      * This method creates at most one session per keyspace.
      */
    def pipeWithKeyspace[F[_], Key, Value](
      implicit cluster: core.Cluster,
      queryable: Queryable[Key, Value],
      async: Async[F]
    ): Pipe[F, (String, Key),  Stream[F, Value]] = {
      (s: Stream[F, (String, Key)]) =>
        s.zip(sessionProviders).flatMap {
          case ((keyspace, key), sessionProvider) =>
            for {
              session <- Stream.eval(sessionProvider(keyspace))
            } yield queryable.query(key).stream[F](session, async)
        }
    }

  }
}
