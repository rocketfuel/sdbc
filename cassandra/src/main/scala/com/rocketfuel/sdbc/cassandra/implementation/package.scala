package com.rocketfuel.sdbc.cassandra

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import fs2.util.Async
import scala.concurrent.{ExecutionContext, Future, Promise}

package object implementation {

  private[sdbc] def toScalaFuture[T](f: ListenableFuture[T])(implicit ec: ExecutionContext): Future[T] = {
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

  private[sdbc] def toAsync[F[_], T](f: ListenableFuture[T])(implicit async: Async[F]): F[T] = {
    async.async { register =>
      async.delay {
        val callback = new FutureCallback[T] {
          override def onFailure(t: Throwable): Unit = register(Left(t))

          override def onSuccess(result: T): Unit = register(Right(result))
        }

        Futures.addCallback(f, callback)
      }
    }
  }

}
