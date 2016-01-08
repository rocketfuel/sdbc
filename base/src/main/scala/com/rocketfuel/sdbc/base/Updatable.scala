package com.rocketfuel.sdbc.base

import com.rocketfuel.sdbc.base

trait Updatable {

  type Connection

  type Update <: base.Update[Connection]

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
