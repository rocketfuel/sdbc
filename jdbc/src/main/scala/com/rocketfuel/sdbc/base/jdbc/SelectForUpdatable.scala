package com.rocketfuel.sdbc.base.jdbc

trait SelectForUpdatable {
  self: DBMS with Connection =>

  trait SelectForUpdatable[Key] {
    def selectForUpdate(key: Key): SelectForUpdate
  }

  object SelectForUpdatable {
    def apply[Key](f: Key => SelectForUpdate): SelectForUpdatable[Key] =
      new SelectForUpdatable[Key] {
        override def selectForUpdate(key: Key): SelectForUpdate =
          f(key)
      }

    def iterator[Key](
      key: Key
    )(implicit selectable: SelectForUpdatable[Key],
      connection: Connection
    ): CloseableIterator[UpdateableRow] = {
      selectable.selectForUpdate(key).iterator()
    }
  }

}
