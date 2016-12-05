package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logger
import fs2.Stream
import fs2.util.Async
import shapeless.HList

trait SelectForUpdate {
  self: DBMS with Connection =>

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
      def constant(rowUpdater: Key => UpdatableRow => Unit = Function.const(q.rowUpdater)): SelectForUpdatable[Key] =
        SelectForUpdatable[Key](Function.const(q), rowUpdater)

      def parameters(toParameters: Key => Parameters, rowUpdater: Key => UpdatableRow => Unit = Function.const(q.rowUpdater)): SelectForUpdatable[Key] =
        SelectForUpdatable(key => q.onParameters(toParameters(key)), (key: Key) => rowUpdater(key))

      def product[
        Repr <: HList,
        HMapKey <: Symbol,
        AsParameters <: HList
      ](rowUpdater: Key => UpdatableRow => Unit = Function.const(q.rowUpdater)
      )(implicit p: Parameters.Products[Key, Repr, HMapKey, AsParameters]
      ): SelectForUpdatable[Key] =
        parameters(Parameters.product(_), key => rowUpdater(key))

      def record[
        Repr <: HList,
        HMapKey <: Symbol,
        AsParameters <: HList
      ](rowUpdater: Key => UpdatableRow => Unit = Function.const(q.rowUpdater)
      )(implicit p: Parameters.Records[Repr, HMapKey, AsParameters],
        ev: Repr =:= Key
      ): SelectForUpdatable[Key] =
        parameters(key => Parameters.record(key.asInstanceOf[Repr]), key => rowUpdater(key))
    }

  }

  object SelectForUpdate
    extends Logger {

    override protected def logClass: Class[_] = classOf[com.rocketfuel.sdbc.base.jdbc.SelectForUpdate]

    val defaultUpdater =
      Function.const[Unit, UpdatableRow](()) _

    def update[A](
      statement: CompiledStatement,
      parameterValues: Parameters = Parameters.empty,
      rowUpdater: UpdatableRow => Unit
    )(implicit connection: Connection
    ): UpdatableRow.Summary = {
      logRun(statement, parameterValues, rowUpdater)
      val executed = QueryMethods.executeForUpdate(statement, parameterValues)
      StatementConverter.updatedResults(executed, rowUpdater)
    }

    def pipe[F[_]](
      statement: CompiledStatement,
      parameters: Parameters = Parameters.empty,
      updater: UpdatableRow => Unit
    )(implicit async: Async[F]
    ): Pipe[F] =
      Pipe(statement, parameters, updater)

    def sink[F[_]](
      statement: CompiledStatement,
      parameters: Parameters = Parameters.empty,
      updater: UpdatableRow => Unit
    )(implicit async: Async[F]
    ): Ignore.Sink[F] =
      Ignore.Sink(statement, parameters)

    case class Pipe[F[_]](
      statement: CompiledStatement,
      defaultParameters: Parameters = Parameters.empty,
      updater: UpdatableRow => Unit
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
      log.debug(s"""query "${compiledStatement.originalQueryText}", parameters $parameters""")
      if (update eq defaultUpdater)
        log.warn("Update function was not set.")
    }

  }

}
