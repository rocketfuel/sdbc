package com.rocketfuel.sdbc.base.jdbc

import fs2.util.Async

trait Ignorable {
  self: DBMS with Connection =>

  trait Ignorable[Key] extends (Key => Ignore)

  object Ignorable {
    def apply[Key](implicit i: Ignorable[Key]): Ignorable[Key] = i

    implicit def create[Key](f: Key => Ignore): Ignorable[Key] =
      new Ignorable[Key] {
        override def apply(key: Key): Ignore =
          f(key)
      }

    def ignore[Key](
      key: Key
    )(implicit ignorable: Ignorable[Key],
      connection: Connection
    ): Unit = {
      ignorable(key).ignore()
    }

    def sink[F[_], Key](
      key: Key
    )(implicit async: Async[F],
      ignorable: Ignorable[Key]
    ): Ignore.Sink[F] = {
      ignorable(key).sink[F]
    }

    trait Partable {
      implicit def ignorablePartable[Key](implicit ignorable: Ignorable[Key]): Batch.Partable[Key] = {
        (key: Key) =>
          val query = ignorable(key)
          Ignore.partable(query)
      }
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
