package com.rocketfuel.sdbc

import com.google.common.util.concurrent._
import fs2.util.Async
import java.util.concurrent.{Executor, ForkJoinPool}
import scala.concurrent._

package object cassandra {

  def toScalaFuture[T](f: ListenableFuture[T])(implicit ec: ExecutionContext): Future[T] = {
    //Thanks http://stackoverflow.com/questions/18026601/listenablefuture-to-scala-future
    val p = Promise[T]()

    val pCallback = new FutureCallback[T] {
      override def onFailure(t: Throwable): Unit = {
        p.failure(t)
      }

      override def onSuccess(result: T): Unit = {
        p.success(result)
      }
    }

    Futures.addCallback(f, pCallback)

    p.future
  }

  /**
    * The executor used when creating callbacks from Cassandra
    * Futures to Async.
    */
  private[cassandra] val executor: Executor =
    System.getProperty("sdbc.cassandra.async.executor", "scala-global") match {
      case "direct" =>
        MoreExecutors.directExecutor()
      case "scala-global" =>
        ExecutionContext.global
      case "fork-join-common" =>
        ForkJoinPool.commonPool()
      case _ =>
        throw new RuntimeException("unknown value for sdbc.cassandra.async.executor")
    }

  def toAsync[F[_], T](f: => ListenableFuture[T])(implicit async: Async[F]): F[T] = {
    async.async { register =>
      async.delay {
        val callback = new FutureCallback[T] {
          override def onFailure(t: Throwable): Unit = register(Left(t))

          override def onSuccess(result: T): Unit = register(Right(result))
        }

        Futures.addCallback(f, callback, executor)
      }
    }
  }

}
