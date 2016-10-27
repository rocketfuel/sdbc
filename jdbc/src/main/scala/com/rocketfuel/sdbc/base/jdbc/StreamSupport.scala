package com.rocketfuel.sdbc.base.jdbc

import fs2.util.Async
import fs2.Stream

trait StreamSupport {
  self: DBMS with Connection =>

  protected def withConnectionOne[
    F[_],
    T
  ](task: Connection => F[T]
  )(implicit pool: Pool,
    a: Async[F]
  ): Stream[F, T] = {
    withConnection[F, T](connection => Stream.eval(task(connection)))
  }

  protected def withConnection[
    F[_],
    T
  ](task: Connection => Stream[F, T]
  )(implicit pool: Pool,
    a: Async[F]
  ): Stream[F, T] = {
    Stream.bracket[F, Connection, T](
      r = a.delay(pool.getConnection())
    )(use = connection => task(connection),
      release = connection => a.delay(connection.close())
    )
  }

}
