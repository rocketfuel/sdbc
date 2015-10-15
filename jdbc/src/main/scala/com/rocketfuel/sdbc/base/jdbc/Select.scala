package com.rocketfuel.sdbc.base.jdbc

import java.io.Closeable
import java.sql._
import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.{Logging, CompiledStatement}
import scala.collection.generic.CanBuildFrom

case class Select[T] private[jdbc] (
  override val statement: CompiledStatement,
  override val parameterValues: Map[String, Option[Any]]
)(implicit val converter: Row => T,
  parameterSetter: ParameterSetter
) extends base.Select[Connection, T]
  with ParameterizedQuery[Select[T]]
  with Logging {

  private def executeQuery()(implicit connection: Connection): ResultSet = {
    logger.debug(s"""Selecting "$originalQueryText" with parameters $parameterValues.""")
    val prepared = prepare(
      queryText = queryText,
      parameterValues = parameterValues,
      parameterPositions = parameterPositions
    )

    prepared.executeQuery()
  }

  /**
   * Retrieve a result set as an iterator of values.
   * The iterator will close the underlying ResultSet after retrieving the final row.
   * The iterator has a close method, so you can close it manually if you don't wish
   * to consume all the results.
   * @param connection
   * @return
   */
  override def iterator()(implicit connection: Connection): Iterator[T] with Closeable = {
    new Iterator[T] with Closeable {
      private val resultRows = executeQuery().iterator()
      private val mappedRows = resultRows.map(converter)

      override def hasNext: Boolean = mappedRows.hasNext

      override def next(): T = mappedRows.next()

      override def close(): Unit = resultRows.close()
    }
  }

  /**
   * Gets the first row from the result set, if one exists. The result set
   * is automatically closed.
   * @param connection
   * @return
   */
  override def option()(implicit connection: Connection): Option[T] = {
    val results = iterator()

    val value = results.to[Stream].headOption

    results.close()

    value
  }

  override def to[F[_]](implicit
    connection: Connection,
    cbf: CanBuildFrom[Nothing, T, F[T]]
  ): F[T] = {
    val rows = iterator()
    try {
      rows.to[F]
    } finally {
      rows.close()
    }
  }

  override protected def subclassConstructor(
    statement: CompiledStatement,
    parameterValues: Map[String, Option[Any]]
  ): Select[T] = {
    Select[T](
      statement,
      parameterValues
    )
  }
}

object Select {

  def apply[T](
    queryText: String,
    hasParameters: Boolean = true
  )(implicit compositeGetter: CompositeGetter[T],
    parameterSetter: ParameterSetter
  ): Select[T] = {
    implicit def getter(row: Row): T = {
      compositeGetter.getter(row, 0)
    }

    Select[T](
      statement = CompiledStatement(queryText, hasParameters),
      parameterValues = Map.empty[String, Option[Any]]
    )
  }

}
