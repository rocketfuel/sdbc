package com.rocketfuel.sdbc.base.jdbc

import fs2.util.Async

trait MultiQueryable {
  self: DBMS with Connection with MultiQuery =>

  trait MultiQueryable[Key, Result] {
    def multiQuery(key: Key): MultiQuery[Result]
  }

  object MultiQueryable {
    def apply[Key, Value](f: Key => MultiQuery[Value]): MultiQueryable[Key, Value] =
      new MultiQueryable[Key, Value] {
        override def multiQuery(key: Key): MultiQuery[Value] =
          f(key)
      }

    def result[Key, Result](
      key: Key
    )(implicit multiQueryable: MultiQueryable[Key, Result],
      connection: Connection
    ): Result = {
      multiQueryable.multiQuery(key).result()
    }

    def pipe[F[_], Key, Result](
      key: Key
    )(implicit async: Async[F],
      multiQueryable: MultiQueryable[Key, Result]
    ): MultiQuery.Pipe[F, Result] = {
      multiQueryable.multiQuery(key).pipe[F]
    }

    def sink[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      multiQueryable: MultiQueryable[Key, _]
    ): Ignore.Sink[F] = {
      multiQueryable.multiQuery(key).sink[F]
    }
  }

}
