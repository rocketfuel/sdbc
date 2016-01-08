package com.rocketfuel.sdbc.base

import com.rocketfuel.sdbc.base

trait Executable {

  type Connection

  type Execute <: base.Execute[Connection]

  trait Executable[Key] {
    def execute(key: Key): Execute
  }

  def execute[Key](key: Key)(implicit
    executable: Executable[Key],
    connection: Connection
  ): Unit = {
    executable.execute(key).execute()
  }

}
