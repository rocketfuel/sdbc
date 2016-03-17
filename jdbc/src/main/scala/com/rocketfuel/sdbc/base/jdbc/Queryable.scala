package com.rocketfuel.sdbc.base.jdbc

trait Queryable {
  self: DBMS =>

  trait Queryable[Key, Result] {
    def query(key: Key): Query[Result]
  }

  def run[Key, Result](
    key: Key
  )(implicit queryable: Queryable[Key, Result],
    connection: Connection
  ): Result = {
    queryable.query(key).run()
  }

}
