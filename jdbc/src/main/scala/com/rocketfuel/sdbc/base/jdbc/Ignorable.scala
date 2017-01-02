package com.rocketfuel.sdbc.base.jdbc

import fs2.util.Async

trait Ignorable {
  self: DBMS with Connection =>

  trait Ignorable[Key] extends Queryable[Ignore, Key] {
    def ignore(key: Key): Ignore
  }

  object Ignorable {
    def apply[Key](implicit i: Ignorable[Key]): Ignorable[Key] = i

    def apply[Key](f: Key => Ignore): Ignorable[Key] =
      new Ignorable[Key] {
        override def query(key: Key): Ignore =
          f(key)

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
      ignorable: Ignorable[Key]
    ): Ignore.Sink[F] = {
      ignorable.ignore(key).sink[F]
    }

    trait syntax {

      implicit class IgnorableSyntax[Key](key: Key)(implicit ignorable: Ignorable[Key]) {
        def ignore()(implicit connection: Connection): Unit = {
          Ignorable.ignore(key)
        }

        def ignoreSink[F[_]](implicit async: Async[F]): Ignore.Sink[F] = {
          Ignorable.sink(key)
        }
      }
    }

    object syntax extends syntax
  }

}
