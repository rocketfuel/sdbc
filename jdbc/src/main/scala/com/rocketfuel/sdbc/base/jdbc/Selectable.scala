package com.rocketfuel.sdbc.base.jdbc

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

}
