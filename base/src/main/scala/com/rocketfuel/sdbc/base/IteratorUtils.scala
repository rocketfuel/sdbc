package com.rocketfuel.sdbc.base

import fs2.Stream
import fs2.util.Async

object IteratorUtils {

  def toStream[F[_], A](i: F[Iterator[A]])(implicit a: Async[F]): Stream[F, A] = {
    for {
      iterator <- Stream.eval(i)
      elem <- Stream.unfold[F, Iterator[A], A](iterator)(i =>
        if (i.hasNext)
          Some((i.next, i))
        else None
      )
    } yield elem
  }

}
