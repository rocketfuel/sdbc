package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logger
import com.rocketfuel.sdbc.base.jdbc.resultset.Row
import fs2.Stream
import fs2.util.Async
import java.io.InputStream
import java.net.URL
import java.nio.file.Path
import java.sql.ResultSet
import shapeless.HList

trait SelectForUpdate {
  self: DBMS with Connection =>

  class UpdatableRow private[sdbc](
    underlying: ResultSet,
    columnNames: IndexedSeq[String],
    columnIndexes: Map[String, Int]
  ) extends ConnectedRow(underlying, columnNames, columnIndexes) {

    def update[T](columnIndex: Index, x: T)(implicit updater: Updater[T]): Unit = {
      updater(this, columnIndex(this), x)
    }

    def summary: UpdatableRow.Summary =
      UpdatableRow.Summary(
        deletedRows = this.deletedRows,
        insertedRows = this.insertedRows,
        updatedRows = this.updatedRows
      )

  }

  object UpdatableRow {
    def apply(resultSet: ResultSet): UpdatableRow = {
      val columnNames = Row.columnNames(resultSet.getMetaData)
      val columnIndexes = Row.columnIndexes(columnNames)

      new UpdatableRow(
        underlying = resultSet,
        columnNames = columnNames,
        columnIndexes = columnIndexes
      )
    }

    def iterator(resultSet: ResultSet): CloseableIterator[UpdatableRow]  = {
      val row = UpdatableRow(resultSet)
      resultSet.iterator().map(Function.const(row))
    }

    def iterator(row: UpdatableRow): CloseableIterator[UpdatableRow]  = {
      row.underlying.iterator().map(Function.const(row))
    }

    /**
      *
      * @param deletedRows how many times deleteRow() was called
      * @param insertedRows how many times insertRow() was called
      * @param updatedRows how many times updateRow() was called
      */
    case class Summary(
      deletedRows: Long = 0L,
      insertedRows: Long = 0L,
      updatedRows: Long = 0L
    )

    object Summary {
      lazy val empty = Summary()
    }

  }

  case class SelectForUpdate(
    override val statement: CompiledStatement,
    override val parameters: Parameters = Parameters.empty,
    rowUpdater: UpdatableRow => Unit = SelectForUpdate.defaultUpdater
  ) extends IgnorableQuery[SelectForUpdate] {
    q =>

    override def subclassConstructor(parameters: Parameters): SelectForUpdate = {
      copy(parameters = parameters)
    }

    def update()(implicit connection: Connection): UpdatableRow.Summary = {
      SelectForUpdate.update(statement, parameters, rowUpdater)
    }

    def pipe[F[_]](implicit async: Async[F]): SelectForUpdate.Pipe[F] =
      SelectForUpdate.pipe[F](statement, parameters, rowUpdater)

    /**
      * Get helper methods for creating [[SelectForUpdatable]]s from this query.
      */
    def selectForUpdatable[Key]: ToSelectForUpdatable[Key] =
      new ToSelectForUpdatable[Key]

    class ToSelectForUpdatable[Key] {
      def constant(): SelectForUpdatable[Key] =
        SelectForUpdatable[Key](Function.const(q) _)

      def parameters(toParameters: Key => Parameters): SelectForUpdatable[Key] =
        SelectForUpdatable(key => q.onParameters(toParameters(key)))

      def product[
        Repr <: HList,
        HMapKey <: Symbol,
        AsParameters <: HList
      ](implicit p: Parameters.Products[Key, Repr, HMapKey, AsParameters]
      ): SelectForUpdatable[Key] =
        parameters(Parameters.product(_))

      def record[
        Repr <: HList,
        HMapKey <: Symbol,
        AsParameters <: HList
      ](implicit p: Parameters.Records[Repr, HMapKey, AsParameters],
        ev: Repr =:= Key
      ): SelectForUpdatable[Key] =
        parameters(key => Parameters.record(key.asInstanceOf[Repr]))
    }

  }

  object SelectForUpdate
    extends Logger {

    def readInputStream(
      stream: InputStream,
      rowUpdater: UpdatableRow => Unit = SelectForUpdate.defaultUpdater
    )(implicit codec: scala.io.Codec = scala.io.Codec.default
    ): SelectForUpdate = {
      SelectForUpdate(CompiledStatement.readInputStream(stream), rowUpdater = rowUpdater)
    }

    def readUrl(
      u: URL,
      rowUpdater: UpdatableRow => Unit = SelectForUpdate.defaultUpdater
    )(implicit codec: scala.io.Codec = scala.io.Codec.default
    ): SelectForUpdate = {
      SelectForUpdate(CompiledStatement.readUrl(u), rowUpdater = rowUpdater)
    }

    def readPath(
      path: Path,
      rowUpdater: UpdatableRow => Unit = SelectForUpdate.defaultUpdater
    )(implicit codec: scala.io.Codec = scala.io.Codec.default
    ): SelectForUpdate = {
      SelectForUpdate(CompiledStatement.readPath(path), rowUpdater = rowUpdater)
    }

    def readClassResource(
      clazz: Class[_],
      name: String,
      rowUpdater: UpdatableRow => Unit = SelectForUpdate.defaultUpdater
    )(implicit codec: scala.io.Codec = scala.io.Codec.default
    ): SelectForUpdate = {
      SelectForUpdate(CompiledStatement.readClassResource(clazz, name), rowUpdater = rowUpdater)
    }

    def readResource(
      name: String,
      rowUpdater: UpdatableRow => Unit = SelectForUpdate.defaultUpdater
    )(implicit codec: scala.io.Codec = scala.io.Codec.default
    ): SelectForUpdate = {
      SelectForUpdate(CompiledStatement.readResource(name), rowUpdater = rowUpdater)
    }

    override protected def logClass: Class[_] = classOf[com.rocketfuel.sdbc.base.jdbc.SelectForUpdate]

    val defaultUpdater: UpdatableRow => Unit =
      Function.const(())

    def update[A](
      statement: CompiledStatement,
      parameterValues: Parameters = Parameters.empty,
      rowUpdater: UpdatableRow => Unit = SelectForUpdate.defaultUpdater
    )(implicit connection: Connection
    ): UpdatableRow.Summary = {
      logRun(statement, parameterValues, rowUpdater)
      val executed = QueryMethods.executeForUpdate(statement, parameterValues)
      StatementConverter.updatedResults(executed, rowUpdater)
    }

    def pipe[F[_]](
      statement: CompiledStatement,
      parameters: Parameters = Parameters.empty,
      updater: UpdatableRow => Unit = SelectForUpdate.defaultUpdater
    )(implicit async: Async[F]
    ): Pipe[F] =
      Pipe(statement, parameters, updater)

    def sink[F[_]](
      statement: CompiledStatement,
      parameters: Parameters = Parameters.empty,
      updater: UpdatableRow => Unit = SelectForUpdate.defaultUpdater
    )(implicit async: Async[F]
    ): Ignore.Sink[F] =
      Ignore.Sink(statement, parameters)

    case class Pipe[F[_]](
      statement: CompiledStatement,
      defaultParameters: Parameters = Parameters.empty,
      updater: UpdatableRow => Unit = SelectForUpdate.defaultUpdater
    )(implicit async: Async[F]
    ) {
      private val parameterPipe = Parameters.Pipe[F]

      /**
        * From a stream of parameter lists, independently add each list to the
        * query, execute it, and ignore the results.
        *
        * A connection is taken from the pool for each execution.
        * @return
        */
      def parameters(implicit pool: Pool): fs2.Pipe[F, Parameters, UpdatableRow.Summary] = {
        parameterPipe.combine(defaultParameters).andThen(
          paramStream =>
            for {
              params <- paramStream
              result <-
                StreamUtils.connection {implicit connection =>
                  Stream.eval(async.delay(update(statement, params, updater)))
                }
            } yield result
        )
      }

      def products[
        A,
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        p: Parameters.Products[A, Repr, Key, AsParameters]
      ): fs2.Pipe[F, A, UpdatableRow.Summary] = {
        parameterPipe.products.andThen(parameters)
      }

      def records[
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        r: Parameters.Records[Repr, Key, AsParameters]
      ): fs2.Pipe[F, Repr, UpdatableRow.Summary] = {
        parameterPipe.records.andThen(parameters)
      }
    }

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Parameters,
      update: UpdatableRow => Unit
    ): Unit = {
      QueryCompanion.logRun(log, compiledStatement, parameters)
      if (update eq defaultUpdater)
        log.warn("Update function was not set.")
    }

    implicit val partable: Batch.Partable[SelectForUpdate] =
      (q: SelectForUpdate) => Batch.Part(q.statement, q.parameters)

  }

}
