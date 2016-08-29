package com.rocketfuel.sdbc.cassandra.implementation

private[sdbc] trait SessionMethods {
  self: Cassandra =>

  implicit class SessionMethods(pool: Session) {
    def iterator[T](
      queryText: String,
      parameters: Parameters
    )(implicit converter: RowConverter[T]
    ): Iterator[T] = {
      Query[T](queryText).onParameters(parameters.parameters).iterator()(pool)
    }

    def option[T](
      queryText: String,
      parameters: Parameters
    )(implicit converter: RowConverter[T]
    ): Option[T] = {
      Query[T](queryText).onParameters(parameters.parameters).option()(pool)
    }
  }

}
