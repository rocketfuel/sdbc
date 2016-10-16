package com.rocketfuel.sdbc.base.jdbc

trait Updatable {
  self: DBMS with Connection =>

  trait Updatable[Key] {
    def update(key: Key): Update
  }

  def update[Key](
    key: Key
  )(implicit updatable: Updatable[Key],
    connection: Connection
  ): Long = {
    updatable.update(key).update()
  }

}
