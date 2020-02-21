package com.rocketfuel.sdbc.base

import fs2.{Pure, Stream}

object IteratorUtils {

  def toStream[F[_] >: Pure[_], A](i: F[Iterator[A]]): Stream[F, A] = {
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
