package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logger
import com.rocketfuel.sdbc.base.jdbc.statement.MultiResultConverter
import fs2.Stream
import fs2.util.Async
import java.sql.ResultSet
import shapeless.ops.record.{MapValues, ToMap}
import shapeless.{HList, LabelledGeneric}

/**
  * Add support for queries with multiple result sets, for use with DBMSs
  * that can return more than one ResultSet per statement.
  *
  */
trait MultiQuery extends MultiResultConverter {
  self: DBMS with Connection =>

  case class MultiQuery[A](
    override val statement: CompiledStatement,
    override val parameters: Parameters = Parameters.empty
  )(implicit multiResultConverter: MultiResultConverter[A]
  ) extends IgnorableQuery[MultiQuery[A]] {

    override def subclassConstructor(parameters: Parameters): MultiQuery[A] = {
      copy(parameters = parameters)
    }

    def result()(implicit connection: Connection): A = {
      MultiQuery.run(statement, parameters)
    }

    def pipe[F[_]](implicit async: Async[F]): MultiQuery.Pipe[F, A] =
      MultiQuery.Pipe(statement, parameters)

  }

  object MultiQuery
    extends Logger {

    override protected def logClass: Class[_] = classOf[com.rocketfuel.sdbc.base.jdbc.MultiQuery]

    val defaultResultSetType = ResultSet.TYPE_FORWARD_ONLY

    val defaultResultSetConcurrency = ResultSet.CONCUR_READ_ONLY

    def run[A](
      compiledStatement: CompiledStatement,
      parameters: Parameters = Parameters.empty
    )(implicit connection: Connection,
      multiResultConverter: MultiResultConverter[A]
    ): A = {
      logRun(compiledStatement, parameters)
      QueryMethods.execute(
        compiledStatement,
        parameters,
        multiResultConverter.resultSetType.getOrElse(defaultResultSetType),
        multiResultConverter.resultSetConcurrency.getOrElse(defaultResultSetConcurrency)
      )
    }

    case class Pipe[F[_], A](
      statement: CompiledStatement,
      defaultParameters: Parameters = Parameters.empty
    )(implicit async: Async[F],
      statementConverter: MultiResultConverter[A]
    ) {
      private val parameterPipe = Parameters.Pipe[F]

      def parameters(implicit pool: Pool): fs2.Pipe[F, Parameters, A] = {
        parameterPipe.combine(defaultParameters).andThen(
          paramStream =>
            for {
              params <- paramStream
              result <-
                StreamUtils.connection {implicit connection =>
                  Stream.eval(async.delay(run(statement, params)))
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
        products: Parameters.Products[B, Repr, Key, AsParameters]
      ): fs2.Pipe[F, B, A] = {
        _.map(p => Parameters.product(p)).through(parameters)
      }

      def records[
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        r: Parameters.Records[Repr, Key, AsParameters]
      ): fs2.Pipe[F, Repr, A] = {
        _.map(p => Parameters.record(p)).through(parameters)
      }

    }

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Parameters
    ): Unit = {
      log.debug(s"""query "${compiledStatement.originalQueryText}" parameters $parameters""")
    }

  }

}
