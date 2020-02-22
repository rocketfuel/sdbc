package com.rocketfuel.sdbc.base.jdbc

import cats.effect.Async
import fs2.Stream
import shapeless.HList

trait Update {
  self: DBMS with Connection =>

  /*
  Override this if the DBMS supports getLargeUpdateCount.
  So far, none do.
   */
  protected def getUpdateCount(statement: PreparedStatement): Long = {
    statement.getUpdateCount.toLong
  }

  case class Update(
    override val statement: CompiledStatement,
    override val parameters: Parameters = Parameters.empty
  ) extends IgnorableQuery[Update] {
    q =>

    override protected def subclassConstructor(parameters: Parameters): Update = {
      copy(parameters = parameters)
    }

    def update()(implicit connection: Connection): Long = {
      Update.update(statement, parameters)
    }

    def pipe[F[_]](implicit async: Async[F]): Update.Pipe[F] =
      Update.pipe(statement, parameters)

    /**
      * Get helper methods for creating [[Updatable]]s from this query.
      */
    def updatable[Key]: ToUpdatable[Key] =
      new ToUpdatable[Key]

    class ToUpdatable[Key] {
      def constant: Updatable[Key] =
        Function.const(q) _

      def parameters(toParameters: Key => Parameters): Updatable[Key] =
        key => q.onParameters(toParameters(key))

      def product[
        Repr <: HList,
        HMapKey <: Symbol,
        AsParameters <: HList
      ](implicit p: Parameters.Products[Key, Repr, HMapKey, AsParameters]
      ): Updatable[Key] =
        parameters(Parameters.product(_))

      def record[
        Repr <: HList,
        HMapKey <: Symbol,
        AsParameters <: HList
      ](implicit p: Parameters.Records[Repr, HMapKey, AsParameters],
        ev: Repr =:= Key
      ): Updatable[Key] =
        parameters(key => Parameters.record(key.asInstanceOf[Repr]))
    }

  }

  object Update
    extends QueryCompanion[Update] {

    override protected def ofCompiledStatement(statement: CompiledStatement): Update =
      Update(statement)

    override protected def logClass: Class[_] = classOf[com.rocketfuel.sdbc.base.jdbc.Update]

    def update(
      compiledStatement: CompiledStatement,
      parameters: Parameters = Parameters.empty
    )(implicit connection: Connection
    ): Long = {
      logRun(compiledStatement, parameters)
      val runStatement = QueryMethods.execute(compiledStatement, parameters)
      try StatementConverter.update(runStatement)
      finally runStatement.close()
    }

    def pipe[F[_]](
      statement: CompiledStatement,
      defaultParameters: Parameters = Parameters.empty
    )(implicit async: Async[F]
    ): Pipe[F] =
      Pipe(statement, defaultParameters)

    def sink[F[_]](
      statement: CompiledStatement,
      defaultParameters: Parameters = Parameters.empty
    )(implicit async: Async[F]
    ): Ignore.Sink[F] =
      Ignore.Sink[F](statement, defaultParameters)

    case class Pipe[F[_]](
      statement: CompiledStatement,
      defaultParameters: Parameters = Parameters.empty
    )(implicit async: Async[F]
    ) {
      private val parameterPipe = Parameters.Pipe[F]

      def parameters(implicit pool: Pool): fs2.Pipe[F, Parameters, Long] = {
        parameterPipe.combine(defaultParameters).andThen(
          paramStream =>
            for {
              params <- paramStream
              result <-
                StreamUtils.connection {implicit connection =>
                  Stream.eval(async.delay(update(statement, params)))
                }
            } yield result
        )
      }

      def products[
        B,
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        p: Parameters.Products[B, Repr, Key, AsParameters]
      ): fs2.Pipe[F, B, Long] = {
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

    implicit val partable: Batch.Partable[Update] =
      (q: Update) => Batch.Part(q.statement, q.parameters)

  }

}
