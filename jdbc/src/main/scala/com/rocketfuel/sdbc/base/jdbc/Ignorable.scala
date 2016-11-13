package com.rocketfuel.sdbc.base.jdbc

trait Ignorable {
  self: DBMS with Connection =>

  trait Ignorable[Key] {
    def ignore(key: Key): Ignore
  }

  def ignore[Key](
    key: Key
  )(implicit ignorable: Ignorable[Key],
    connection: Connection
  ): Unit = {
    ignorable.ignore(key).ignore()
  }

}
