package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logging
import fs2.{Stream, pipe}
import fs2.util.Async
import shapeless.ops.record.{MapValues, ToMap}
import shapeless.{HList, LabelledGeneric}

trait Execute {
  self: DBMS with Connection =>

  trait IgnorableQuery[Self <: IgnorableQuery[Self]]
    extends ParameterizedQuery[Self] {

    def execute()(implicit connection: Connection): Unit = {
      Execute.execute(statement, parameters)
    }

    def sink[F[_]](implicit async: Async[F]): Execute.Sink[F] =
      Execute.Sink[F](statement, parameters)

  }

  case class Execute(
    override val statement: CompiledStatement,
    override val parameters: Parameters = Parameters.empty
  ) extends ParameterizedQuery[Execute]
    with IgnorableQuery[Execute] {

    override def execute()(implicit connection: Connection): Unit = {
      Execute.execute(statement, parameters)
    }

    override def subclassConstructor(parameters: Parameters): Execute = {
      copy(parameters = parameters)
    }

  }

  object Execute
    extends Logging {

    def execute(
      statement: CompiledStatement,
      parameters: Parameters = Parameters.empty
    )(implicit connection: Connection
    ): Unit = {
      logRun(statement, parameters)
      val executed = QueryMethods.execute(statement, parameters)
      executed.close()
    }

    case class Sink[F[_]](
      statement: CompiledStatement,
      defaultParameters: Parameters = Parameters.empty
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
      def parameters(implicit pool: Pool): fs2.Sink[F, Parameters] = {
        parameterPipe.combine(defaultParameters).andThen(
          pipe.lift[F, Parameters, Unit] { params =>
            Stream.bracket[F, Connection, Unit](
              r = async.delay(pool.getConnection())
            )(use = {implicit connection: Connection => Stream.eval(async.delay(execute(statement, params)))},
              release = connection => async.delay(connection.close())
            )
          }
        )
      }

      def products[
        A,
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        genericA: LabelledGeneric.Aux[A, Repr],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, AsParameters],
        toMap: ToMap.Aux[AsParameters, Key, ParameterValue]
      ): fs2.Sink[F, A] = {
        parameterPipe.products.andThen(parameters)
      }

      def records[
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, AsParameters],
        toMap: ToMap.Aux[AsParameters, Key, ParameterValue]
      ): fs2.Sink[F, Repr] = {
        parameterPipe.records.andThen(parameters)
      }
    }

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Parameters
    ): Unit = {
      logger.debug(s"""Executing "${compiledStatement.originalQueryText}" with parameters $parameters.""")
    }

  }

}
