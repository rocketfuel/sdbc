package com.rocketfuel.sdbc.base.jdbc

import com.typesafe.config.Config
import com.zaxxer.hikari.HikariConfig
import fs2.util.Async
import fs2.Stream

trait StreamUtils {
  self: DBMS with Connection =>

  object StreamUtils {

    /**
      * Create a Stream from a managed Pool.
      */
    def pool[F[_], O](
      config: Config
    )(use: Pool => Stream[F, O]
    )(implicit async: Async[F]
    ): Stream[F, O] = {
      pool(config.toHikariConfig)(use)
    }

    /**
      * Create a Stream from a managed Pool.
      */
    def pool[F[_], O](
      config: HikariConfig
    )(use: Pool => Stream[F, O]
    )(implicit async: Async[F]
    ): Stream[F, O] = {
      val req = async.delay {
        Pool(config)
      }
      def release(pool: Pool): F[Unit] = {
        async.delay(pool.close())
      }
      Stream.bracket(req)(use, release)
    }

    /**
      * Create a Stream from a managed Connection.
      */
    def connection[F[_], O](
      use: Connection => Stream[F, O]
    )(implicit pool: Pool,
      async: Async[F]
    ): Stream[F, O] = {
      val req = async.delay(pool.getConnection())
      def release(connection: Connection): F[Unit] = {
        async.delay(connection.close())
      }
      Stream.bracket(req)(use, release)
    }

  }

}
