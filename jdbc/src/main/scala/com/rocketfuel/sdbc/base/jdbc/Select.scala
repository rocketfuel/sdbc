package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.{Logger, StreamUtils}
import fs2.{Stream, pipe}
import fs2.util.Async
import shapeless.ops.record.{MapValues, ToMap}
import shapeless.{HList, LabelledGeneric}

trait Select {
  self: DBMS with Connection =>

  /**
    * Represents a query that is ready to be run against a [[Connection]].
    * @param statement is the text of the query. You can supply a String, and it will be converted to a
    *                  [[CompiledStatement]] by [[CompiledStatement!.apply(String)]].
    * @param parameters
    * @param rowConverter
    * @tparam A
    */
  case class Select[A](
    override val statement: CompiledStatement,
    override val parameters: Parameters = Parameters.empty
  )(implicit rowConverter: RowConverter[A]
  ) extends IgnorableQuery[Select[A]] {

    override def subclassConstructor(parameters: Parameters): Select[A] = {
      copy(parameters = parameters)
    }

    def iterator()(implicit connection: Connection): CloseableIterator[A] = {
      Select.iterator(statement, parameters)
    }

    def vector()(implicit connection: Connection): Vector[A] = {
      Select.vector(statement, parameters)
    }

    def option()(implicit connection: Connection): Option[A] = {
      Select.option(statement, parameters)
    }

    def singleton()(implicit connection: Connection): A = {
      Select.singleton(statement, parameters)
    }

    def streamFromConnection[F[_]]()(implicit
      async: Async[F],
      connection: Connection
    ): Stream[F, A] = {
      Select.streamFromConnection[F, A](statement, parameters)
    }

    def streamFromPool[F[_]]()(implicit
      async: Async[F],
      pool: Pool
    ): Stream[F, A] = {
      Select.streamFromPool[F, A](statement, parameters)
    }

    def pipe[F[_]](implicit async: Async[F]): Select.Pipe[F, A] =
      Select.Pipe(statement, parameters)

  }

  object Select
    extends Logger {

    def iterator[A](
      statement: CompiledStatement,
      parameterValues: Parameters = Parameters.empty
    )(implicit connection: Connection,
      rowConverter: RowConverter[A]
    ): CloseableIterator[A] = {
      logRun(statement, parameterValues)
      val executed = QueryMethods.execute(statement, parameterValues)
      StatementConverter.convertedRowIterator[A](executed)
    }

    def option[A](
      statement: CompiledStatement,
      parameterValues: Parameters = Parameters.empty
    )(implicit connection: Connection,
      rowConverter: RowConverter[A]
    ): Option[A] = {
      logRun(statement, parameterValues)
      val executed = QueryMethods.execute(statement, parameterValues)
      try StatementConverter.convertedRowOption(executed)
      finally executed.close()
    }

    def singleton[A](
      statement: CompiledStatement,
      parameterValues: Parameters = Parameters.empty
    )(implicit connection: Connection,
      rowConverter: RowConverter[A]
    ): A = {
      logRun(statement, parameterValues)
      val executed = QueryMethods.execute(statement, parameterValues)
      try StatementConverter.convertedRowSingleton(executed)
      finally executed.close()
    }

    def vector[A](
      statement: CompiledStatement,
      parameterValues: Parameters = Parameters.empty
    )(implicit connection: Connection,
      rowConverter: RowConverter[A]
    ): Vector[A] = {
      logRun(statement, parameterValues)
      val executed = QueryMethods.execute(statement, parameterValues)

      try StatementConverter.convertedRowVector[A](executed)
      finally executed.close()
    }

    def streamFromPool[F[_], A](
      statement: CompiledStatement,
      parameterValues: Parameters = Parameters.empty
    )(implicit async: Async[F],
      pool: Pool,
      rowConverter: RowConverter[A]
    ): Stream[F, A] = {
      StreamUtils.fromCloseableIteratorR[F, Connection, A](
        async.delay(pool.getConnection()),
        {implicit connection: Connection =>
          async.delay(iterator(statement, parameterValues))
        },
        (c: Connection) => async.delay(c.close()))
    }

    def streamFromConnection[F[_], A](
      statement: CompiledStatement,
      parameterValues: Parameters = Parameters.empty
    )(implicit async: Async[F],
      connection: Connection,
      rowConverter: RowConverter[A]
    ): Stream[F, A] = {
      StreamUtils.fromCloseableIterator(async.delay(iterator[A](statement, parameterValues)))
    }

    case class Pipe[F[_], A](
      statement: CompiledStatement,
      defaultParameters: Parameters = Parameters.empty
    )(implicit async: Async[F],
      rowConverter: RowConverter[A]
    ) {
      private val parameterPipe = Parameters.Pipe[F]

      def parameters(implicit pool: Pool): fs2.Pipe[F, Parameters, Stream[F, A]] = {
        parameterPipe.combine(defaultParameters).andThen(
          pipe.lift[F, Parameters, Stream[F, A]] { params =>
            streamFromPool[F, A](statement, params)
          }
        )
      }

      def products[
        B,
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        genericA: LabelledGeneric.Aux[B, Repr],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, AsParameters],
        toMap: ToMap.Aux[AsParameters, Key, ParameterValue]
      ): fs2.Pipe[F, B, Stream[F, A]] = {
        parameterPipe.products.andThen(parameters)
      }

      def records[
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, AsParameters],
        toMap: ToMap.Aux[AsParameters, Key, ParameterValue]
      ): fs2.Pipe[F, Repr, Stream[F, A]] = {
        parameterPipe.records.andThen(parameters)
      }

    }

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Parameters
    ): Unit = {
      log.debug(s"""Selecting "${compiledStatement.originalQueryText}" with parameters $parameters.""")
    }

  }

}
