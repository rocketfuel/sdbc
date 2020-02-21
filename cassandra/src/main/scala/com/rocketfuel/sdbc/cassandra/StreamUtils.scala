package com.rocketfuel.sdbc.cassandra

import cats.effect.Async
import com.datastax.oss.driver.api.core.{CqlSession, CqlSessionBuilder}
import com.rocketfuel.sdbc.base.Logger
import fs2.Stream

object StreamUtils extends Logger {
  /**
    * Create a stream from a managed CqlSession.
    */
  def session[F[_], O](
    builder: CqlSessionBuilder
  )(use: CqlSession => Stream[F, O]
  )(implicit async: Async[F]
  ): Stream[F, O] = {
    val req = toAsync {
      builder.buildAsync()
    }
    def release(session: CqlSession): F[Unit] = {
      async.map(toAsync(session.closeAsync()))(Function.const(()))
    }
    Stream.bracket(req)(release).flatMap(use)
  }
}
