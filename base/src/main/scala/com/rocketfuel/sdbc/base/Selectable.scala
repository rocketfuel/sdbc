package com.rocketfuel.sdbc.base

import com.rocketfuel.sdbc.base

trait Selectable {

  type Connection

  type Select[Value] <: base.Select[Connection, Value]

  trait Selectable[Key, Value] {
    def select(key: Key): Select[Value]
  }

  def iterator[Key, Value](
    key: Key
  )(implicit selectable: Selectable[Key, Value],
    connection: Connection
  ): Iterator[Value] = {
    selectable.select(key).iterator()
  }

}
