package com.rocketfuel.sdbc.base.jdbc

trait SelectForUpdatable {
  self: DBMS =>

  trait SelectForUpdatable[Key] {
    def selectForUpdate(key: Key): SelectForUpdate
  }

  def iteratorForUpdate[Key](
    key: Key
  )(implicit selectable: SelectForUpdatable[Key],
    connection: Connection
  ): CloseableIterator[UpdatableRow] = {
    selectable.selectForUpdate(key).iterator()
  }

}
