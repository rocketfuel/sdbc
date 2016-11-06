package com.rocketfuel.sdbc.base.jdbc

import fs2.util.Async

trait Selectable {
  self: DBMS with Connection =>

  trait Selectable[Key, Result] {
    def select(key: Key): Select[Result]
  }

  def iterator[Key, Result](
    key: Key
  )(implicit selectable: Selectable[Key, Result],
    connection: Connection
  ): CloseableIterator[Result] = {
    selectable.select(key).iterator()
  }

  def seq[Key, Result](
    key: Key
  )(implicit selectable: Selectable[Key, Result],
    connection: Connection
  ): Seq[Result] = {
    selectable.select(key).iterator().toVector
  }

  def option[Key, Result](
    key: Key
  )(implicit selectable: Selectable[Key, Result],
    connection: Connection
  ): Option[Result] = {
    selectable.select(key).option()
  }

  def stream[F[_], Key, Result](
    key: Key
  )(implicit selectable: Selectable[Key, Result],
    pool: Pool,
    async: Async[F]
  ): Select.Pipe[F, Result] = {
    selectable.select(key).pipe[F]
  }

}
