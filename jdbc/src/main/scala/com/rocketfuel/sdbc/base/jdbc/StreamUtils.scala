package com.rocketfuel.sdbc.base.jdbc

import com.typesafe.config.Config
import com.zaxxer.hikari.HikariConfig
import fs2.util.Async
import fs2.Stream
import java.sql.DriverManager
import java.util.Properties
import javax.sql.DataSource

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

    private def connectionAux[F[_], O](
      use: Connection => Stream[F, O],
      req: F[Connection]
    )(implicit async: Async[F]
    ): Stream[F, O] = {
      def release(connection: Connection): F[Unit] = {
        async.delay(connection.close())
      }
      Stream.bracket(req)(use, release)
    }

    /**
      * Create a Stream from an unmanaged Connection.
      */
    def connection[F[_], O](
      use: Connection => Stream[F, O]
    )(implicit pool: Pool,
      async: Async[F]
    ): Stream[F, O] = {
      val req = async.delay(pool.getConnection())
      connectionAux(use, req)
    }

    /**
      * Create a Stream from a managed Connection.
      */
    def connection[F[_], O](
      url: String
    )(use: Connection => Stream[F, O]
    )(implicit async: Async[F]
    ): Stream[F, O] = {
      val req: F[Connection] = async.delay(Connection(DriverManager.getConnection(url)))
      connectionAux(use, req)
    }

    /**
      * Create a Stream from a managed Connection.
      */
    def connection[F[_], O](
      url: String,
      user: String,
      password: String
    )(use: Connection => Stream[F, O]
    )(implicit async: Async[F]
    ): Stream[F, O] = {
      val req = async.delay(Connection(DriverManager.getConnection(url, user, password)))
      connectionAux(use, req)
    }

    /**
      * Create a Stream from a managed Connection.
      */
    def connection[F[_], O](
      url: String,
      info: Properties
    )(use: Connection => Stream[F, O]
    )(implicit async: Async[F]
    ): Stream[F, O] = {
      val req = async.delay(Connection(DriverManager.getConnection(url, info)))
      connectionAux(use, req)
    }

    /**
      * Create a Stream from a managed Connection.
      */
    def connection[F[_], O](
      dataSource: DataSource
    )(use: Connection => Stream[F, O]
    )(implicit async: Async[F]
    ): Stream[F, O] = {
      val req = async.delay(Connection(dataSource.getConnection()))
      connectionAux(use, req)
    }

    /**
      * Create a Stream from a managed Connection.
      */
    def connection[F[_], O](
      dataSource: DataSource,
      user: String,
      password: String
    )(use: Connection => Stream[F, O]
    )(implicit async: Async[F]
    ): Stream[F, O] = {
      val req = async.delay(Connection(dataSource.getConnection(user, password)))
      connectionAux(use, req)
    }

  }

}
