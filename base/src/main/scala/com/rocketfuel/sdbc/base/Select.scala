package com.rocketfuel.sdbc.base

import scala.collection.generic.CanBuildFrom

/**
 * A `Select` is an operation on a resource that produces some values, T.
 * @tparam Connection is the resource that produces values.
 * @tparam T is the values.
 */
abstract class Select[Connection, T] {
  self: Logging =>

  def iterator()(implicit connection: Connection): Iterator[T]

  def option()(implicit connection: Connection): Option[T] = {
    to[Stream].headOption
  }

  def to[F[_]](implicit
    connection: Connection,
    cbf: CanBuildFrom[Nothing, T, F[T]]
  ): F[T] = {
    val rows = iterator()
    rows.to[F]
  }

}
