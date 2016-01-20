package com.rocketfuel.sdbc.base.jdbc

import java.io.Closeable
import java.sql._
import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.{Logging, CompiledStatement}

trait Select {
  self: DBMS =>

  case class Select[T] private[jdbc](
    override val statement: CompiledStatement,
    override val parameterValues: Map[String, ParameterValue]
  )(implicit val converter: RowConverter[T]
  ) extends base.Select[Connection, T]
  with ParameterizedQuery[Select[T]]
  with Logging {
x = this.on
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
    def option()(implicit connection: Connection): Option[T] = {
      val results = iterator()

      val value = results.to[Stream].headOption

      results.close()

      value
    }

    override protected def subclassConstructor(
      statement: CompiledStatement,
      parameterValues: Map[String, ParameterValue]
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
    )(implicit converter: RowConverter[T]
    ): Select[T] = {
      Select[T](
        statement = CompiledStatement(queryText, hasParameters),
        parameterValues = Map.empty[String, ParameterValue]
      )
    }

  }

}
