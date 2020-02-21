package com.rocketfuel.sdbc

import cats.effect.Async
import java.util.concurrent.CompletionStage
import scala.concurrent._

package object cassandra {

  def toScalaFuture[T](f: CompletionStage[T])(implicit ec: ExecutionContext): Future[T] = {
    val p = Promise[T]()

    f.whenComplete(new java.util.function.BiConsumer[T, Throwable] {
      override def accept(t: T, u: Throwable): Unit = {
        if (u == null) {
          p.success(t)
        } else p.failure(u)
      }
    })

    p.future
  }

  def toAsync[F[_], T](f: => CompletionStage[T])(implicit async: Async[F]): F[T] = {
    async.asyncF { register =>
      async.delay[Unit] {
        f.whenComplete(new java.util.function.BiConsumer[T, Throwable] {
          override def accept(t: T, u: Throwable): Unit = {
            if (u == null) {
              register(Right(t))
            } else register(Left(u))
          }
        })
      }
    }
  }

}
