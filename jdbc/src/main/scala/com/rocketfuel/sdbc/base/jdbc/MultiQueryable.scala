package com.rocketfuel.sdbc.base.jdbc

trait MultiQueryable {
  self: DBMS with Connection with MultiQuery =>

  trait MultiQueryable[Key, Result] {
    def run(key: Key): MultiQuery[Result]
  }

  def run[Key, Result](
    key: Key
  )(implicit multiQueryable: MultiQueryable[Key, Result],
    connection: Connection
  ): Result = {
    multiQueryable.run(key).run()
  }

}
