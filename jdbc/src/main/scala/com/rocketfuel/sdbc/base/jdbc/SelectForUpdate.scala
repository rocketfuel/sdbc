package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logger
import fs2.Stream
import fs2.util.Async
import fs2.util.syntax._
import shapeless.HList

trait SelectForUpdate {
  self: DBMS with Connection =>

  case class SelectForUpdate(
    override val statement: CompiledStatement,
    override val parameters: Parameters = Parameters.empty,
    updater: UpdatableRow => Unit = SelectForUpdate.defaultUpdater
  ) extends IgnorableQuery[SelectForUpdate] {

    override def subclassConstructor(parameters: Parameters): SelectForUpdate = {
      copy(parameters = parameters)
    }

    def update()(implicit connection: Connection): Long = {
      SelectForUpdate.update(statement, parameters, updater)
    }

    def pipe[F[_]](implicit async: Async[F]): SelectForUpdate.Pipe[F] =
      SelectForUpdate.pipe[F](statement, parameters, updater)

  }

  object SelectForUpdate
    extends Logger {

    private val defaultUpdater =
      Function.const[Unit, UpdatableRow](()) _

    def update[A](
      statement: CompiledStatement,
      parameterValues: Parameters = Parameters.empty,
      updater: UpdatableRow => Unit = defaultUpdater
    )(implicit connection: Connection
    ): Long = {
      logRun(statement, parameterValues, updater)
      val executed = QueryMethods.executeForUpdate(statement, parameterValues)
      try {
        val i = StatementConverter.updatableResults(executed)
        i.foldLeft(0L){case (count, row) => updater(row); count + 1}
      } finally executed.close()
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
      def parameters(implicit pool: Pool): fs2.Pipe[F, Parameters, Long] = {
        parameterPipe.combine(defaultParameters).andThen(
          paramStream =>
            for {
              params <- paramStream
              result <-
                StreamUtils.connection {implicit connection =>
                  Stream.eval(async.delay(SelectForUpdate.update(statement, params, updater)))
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
      ): fs2.Pipe[F, A, Long] = {
        parameterPipe.products.andThen(parameters)
      }

      def records[
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        r: Parameters.Records[Repr, Key, AsParameters]
      ): fs2.Pipe[F, Repr, Long] = {
        parameterPipe.records.andThen(parameters)
      }
    }

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Parameters,
      update: UpdatableRow => Unit
    ): Unit = {
      log.debug(s"""Selecting for update "${compiledStatement.originalQueryText}" with parameters $parameters.""")
      if (update eq defaultUpdater)
        log.warn("Update function was not set.")
    }

  }

}
