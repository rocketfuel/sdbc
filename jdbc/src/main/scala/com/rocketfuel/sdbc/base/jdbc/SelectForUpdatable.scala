package com.rocketfuel.sdbc.base.jdbc

trait SelectForUpdatable {
  self: DBMS =>

  trait SelectForUpdatable[Key] {
    def select(key: Key): SelectForUpdate
  }

  def iteratorForUpdate[Key](
    key: Key
  )(implicit selectForUpdatable: SelectForUpdatable[Key],
    connection: Connection
  ): Iterator[UpdatableRow] = {
    selectForUpdatable.select(key).iterator()
  }

}
