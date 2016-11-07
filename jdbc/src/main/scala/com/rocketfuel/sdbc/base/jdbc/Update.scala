package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logging
import fs2.util.Async
import fs2.Stream
import shapeless.ops.record.{MapValues, ToMap}
import shapeless.{HList, LabelledGeneric}

trait Update {
  self: DBMS with Connection =>

  case class Update(
    override val statement: CompiledStatement,
    override val parameters: Parameters = Parameters.empty
  ) extends IgnorableQuery[Update] {

    override def subclassConstructor(parameters: Parameters): Update = {
      copy(parameters = parameters)
    }

    def update()(implicit connection: Connection): Long = {
      Update.update(statement, parameters)
    }

    def pipe[F[_]](implicit async: Async[F]): Update.Pipe[F] =
      Update.Pipe(statement, parameters)

  }

  object Update
    extends Logging {

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
                Stream.bracket(
                  r = async.delay(pool.getConnection())
                )(use = {implicit connection => Stream.eval(async.delay(update(statement, params)))},
                  release = connection => async.delay(connection.close())
                )
            } yield result
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
      ): fs2.Pipe[F, B, Long] = {
        parameterPipe.products.andThen(parameters)
      }

      def records[
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, AsParameters],
        toMap: ToMap.Aux[AsParameters, Key, ParameterValue]
      ): fs2.Pipe[F, Repr, Long] = {
        parameterPipe.records.andThen(parameters)
      }

    }

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Parameters
    ): Unit = {
      logger.debug(s"""Updating "${compiledStatement.originalQueryText}" with parameters $parameters.""")
    }

  }

}
