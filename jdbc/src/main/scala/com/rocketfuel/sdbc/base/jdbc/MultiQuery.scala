package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logging
import com.rocketfuel.sdbc.base.jdbc.statement.MultiStatementConverter
import fs2.Stream
import fs2.util.Async
import shapeless.ops.record.{MapValues, ToMap}
import shapeless.{HList, LabelledGeneric}

/**
  * Add support for queries with multiple result sets, for use with DBMSs
  * that can return more than one ResultSet per statement.
  *
  */
trait MultiQuery extends MultiStatementConverter {
  self: DBMS with Connection =>

  case class MultiQuery[A](
    override val statement: CompiledStatement,
    override val parameters: Parameters
  )(implicit statementConverter: MultiStatementConverter[A]
  ) extends IgnorableQuery[MultiQuery[A]] {

    override def subclassConstructor(parameters: Parameters): MultiQuery[A] = {
      copy(parameters = parameters)
    }

    def run()(implicit connection: Connection): A = {
      MultiQuery.run(statement, parameters)
    }

    def pipe[F[_]](implicit async: Async[F]): MultiQuery.Pipe[F, A] =
      MultiQuery.pipe(statement, parameters)

  }

  object MultiQuery
    extends Logging {

    def run[A](
      compiledStatement: CompiledStatement,
      parameters: Parameters
    )(implicit connection: Connection,
      statementConverter: MultiStatementConverter[A]
    ): A = {
      logRun(compiledStatement, parameters)

      val bound = QueryMethods.execute(compiledStatement, parameters)

      bound.execute()
      bound
    }

    class Pipe[F[_], A] private[MultiQuery] (
      statement: CompiledStatement,
      defaultParameters: Parameters
    )(implicit async: Async[F],
      statementConverter: MultiStatementConverter[A]
    ) {
      private val parameterPipe = Parameters.Pipe[F]

      def parameters(implicit pool: Pool): fs2.Pipe[F, Parameters, A] = {
        parameterPipe.combine(defaultParameters).andThen(
          paramStream =>
            for {
              params <- paramStream
              result <-
              Stream.bracket(
                r = async.delay(pool.getConnection())
              )(use = {implicit connection => Stream.eval(async.delay(run(statement, params)))},
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
      ): fs2.Pipe[F, B, A] = {
        _.map(p => Parameters.product(p)).through(parameters)
      }

      def records[
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, AsParameters],
        toMap: ToMap.Aux[AsParameters, Key, ParameterValue]
      ): fs2.Pipe[F, Repr, A] = {
        _.map(p => Parameters.record(p)).through(parameters)
      }

    }

    def pipe[F[_], A](
      statement: CompiledStatement,
      defaultParameters: Parameters = Parameters.empty
    )(implicit async: Async[F],
      statementConverter: MultiStatementConverter[A]
    ): Pipe[F, A] =
      new Pipe[F, A](statement, defaultParameters)

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Parameters
    ): Unit = {
      logger.debug(s"""Executing "${compiledStatement.originalQueryText}" with parameters $parameters.""")
    }

  }

}
