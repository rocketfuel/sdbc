package com.rocketfuel.sdbc.cassandra

import com.datastax.driver.core.Session
import com.rocketfuel.sdbc.Cassandra.Parameters

class SessionMethods(val pool: Session) extends AnyVal {
  def iterator[T](
    queryText: String,
    parameters: Parameters
  )(implicit converter: RowConverter[T]
  ): Iterator[T] = {
    Query[T](queryText).onParameters(parameters).iterator()(pool)
  }

  def option[T](
    queryText: String,
    parameters: Parameters
  )(implicit converter: RowConverter[T]
  ): Option[T] = {
    Query[T](queryText).onParameters(parameters).option()(pool)
  }
}

trait ToSessionMethods {

  implicit def toSessionMethods(session: Session): SessionMethods =
    new SessionMethods(session)

}
