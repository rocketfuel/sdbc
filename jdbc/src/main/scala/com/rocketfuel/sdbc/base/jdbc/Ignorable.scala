package com.rocketfuel.sdbc.base.jdbc

import fs2.util.Async

trait Ignorable {
  self: DBMS with Connection =>

  trait Ignorable[Key] {
    def ignore(key: Key): Ignore
  }

  object Ignorable {
    def apply[Key](f: Key => Ignore): Ignorable[Key] =
      new Ignorable[Key] {
        override def ignore(key: Key): Ignore =
          f(key)
      }

    def ignore[Key](
      key: Key
    )(implicit ignorable: Ignorable[Key],
      connection: Connection
    ): Unit = {
      ignorable.ignore(key).ignore()
    }

    def sink[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      ignorable: Ignorable[Key],
      connection: Connection
    ): Ignore.Sink[F] = {
      ignorable.ignore(key).sink[F]
    }
  }

}
