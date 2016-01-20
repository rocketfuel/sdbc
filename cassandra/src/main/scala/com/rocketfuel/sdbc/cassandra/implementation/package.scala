package com.rocketfuel.sdbc.cassandra

import com.google.common.util.concurrent.{Futures, FutureCallback, ListenableFuture}
import scala.concurrent.{Promise, Future, ExecutionContext}
import scalaz.{\/-, -\/}
import scalaz.concurrent.Task

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

  private[sdbc] def toTask[T](f: ListenableFuture[T]): Task[T] = {
    Task.async { register =>
      val callback = new FutureCallback[T] {
        override def onFailure(t: Throwable): Unit = register(-\/(t))

        override def onSuccess(result: T): Unit = register(\/-(result))
      }

      Futures.addCallback(f, callback)
    }
  }

}
