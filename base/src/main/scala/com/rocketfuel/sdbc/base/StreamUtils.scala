package com.rocketfuel.sdbc.base

import fs2.Stream
import fs2.util.Async

private[sdbc] object StreamUtils {

  def fromIterator[F[_], A](i: Iterator[A])(implicit a: Async[F]): Stream[F, A] = {
    val step: F[Option[A]] = a.delay {
      if (i.hasNext) Some(i.next)
      else None
    }

    Stream.eval(step).repeat.through(fs2.pipe.unNoneTerminate)
  }

  def fromIterator[F[_], A](i: F[Iterator[A]])(implicit a: Async[F]): Stream[F, A] = {
    for {
      iterator <- Stream.eval(i)
      elem <- fromIterator(iterator)
    } yield elem
  }

  def fromIteratorR[F[_], R, A](getR: F[R], getI: R => F[Iterator[A]], close: R => F[Unit])(implicit async: Async[F]): Stream[F, A] = {
    Stream.bracket[F, R, A](getR)(
      resource => fromIterator(getI(resource)),
      resource => close(resource)
    )
  }

}
