package com.rocketfuel.sdbc.base

import fs2.Stream
import fs2.util.Async

private[sdbc] object IteratorUtils {

  def fromIterator[F[_], A](i: F[Iterator[A]])(implicit a: Async[F]): Stream[F, A] = {
    for {
      iterator <- Stream.eval(i)
      elem <- Stream.unfold[F, Iterator[A], A](iterator)(i =>
        if (i.hasNext)
          Some((i.next, i))
        else None
      )
    } yield elem
  }

  def fromCloseableIterator[F[_], A](i: F[CloseableIterator[A]])(implicit a: Async[F]): Stream[F, A] = {
    Stream.bracket[F, CloseableIterator[A], A](i)(i => fromIterator[F, A](a.pure(i.toIterator)), i => a.delay(i.close()))
  }

}
