package com.rocketfuel.sdbc.base.jdbc

trait Queryable {
  self: DBMS with Connection =>

  trait Queryable[Q <: CompiledParameterizedQuery[Q], Key] {
    def query(key: Key): Q
  }

  object Queryable {
    def apply[Q <: CompiledParameterizedQuery[Q], Key](implicit q: Queryable[Q, Key]): Queryable[Q, Key] =
      q

    def apply[Q <: CompiledParameterizedQuery[Q], Key](f: Key => Q): Queryable[Q, Key] =
      new Queryable[Q, Key] {
        override def query(key: Key): Q = f(key)
      }
  }

}
