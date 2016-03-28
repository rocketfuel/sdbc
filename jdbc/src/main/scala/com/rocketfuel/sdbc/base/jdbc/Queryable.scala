package com.rocketfuel.sdbc.base.jdbc

trait Queryable {
  self: DBMS =>

  trait Queryable[Key, InnerResult, OuterResult <: Result[InnerResult]] {
    def query(key: Key): Query[InnerResult, OuterResult]
  }

  def run[Key, InnerResult, OuterResult <: Result[InnerResult]](
    key: Key
  )(implicit queryable: Queryable[Key, InnerResult, OuterResult],
    connection: Connection
  ) = {
    val Run(closer, result) = queryable.query(key)
    try result
    finally closer.close()
  }

}
