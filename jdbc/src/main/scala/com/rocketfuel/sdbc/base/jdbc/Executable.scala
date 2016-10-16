package com.rocketfuel.sdbc.base.jdbc

trait Executable {
  self: DBMS with Connection =>

  trait Executable[Key] {
    def execute(key: Key): Execute
  }

  def execute[Key](
    key: Key
  )(implicit executable: Executable[Key],
    connection: Connection
  ): Unit = {
    executable.execute(key).execute()
  }

}
