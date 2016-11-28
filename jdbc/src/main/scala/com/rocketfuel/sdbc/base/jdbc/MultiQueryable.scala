package com.rocketfuel.sdbc.base.jdbc

import fs2.util.Async

trait MultiQueryable {
  self: DBMS with Connection with MultiQuery =>

  trait MultiQueryable[Key, Result] {
    def multiQueryable(key: Key): MultiQuery[Result]
  }

  object MultiQueryable {
    def result[Key, Result](
      key: Key
    )(implicit multiQueryable: MultiQueryable[Key, Result],
      connection: Connection
    ): Result = {
      multiQueryable.multiQueryable(key).result()
    }

    def pipe[F[_], Key, Result](
      key: Key
    )(implicit async: Async[F],
      multiQueryable: MultiQueryable[Key, Result],
      connection: Connection
    ): MultiQuery.Pipe[F, Result] = {
      multiQueryable.multiQueryable(key).pipe[F]
    }

    def sink[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      multiQueryable: MultiQueryable[Key, _],
      connection: Connection
    ): Ignore.Sink[F] = {
      multiQueryable.multiQueryable(key).sink[F]
    }
  }

}
