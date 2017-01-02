package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logger
import fs2.util.Async
import fs2.Stream
import shapeless.HList

trait Delete {
  self: DBMS with Connection =>

  case class Delete(
    override val statement: CompiledStatement,
    override val parameters: Parameters = Parameters.empty
  ) extends IgnorableQuery[Delete] {
    q =>

    override protected def subclassConstructor(parameters: Parameters): Delete = {
      copy(parameters = parameters)
    }

    def delete()(implicit connection: Connection): Long = {
      Delete.delete(statement, parameters)
    }

    def pipe[F[_]](implicit async: Async[F]): Delete.Pipe[F] =
      Delete.Pipe(statement, parameters)

    /**
      * Get helper methods for creating [[Deletable]]s from this query.
      */
    def deletable[Key]: ToDeletable[Key] =
      new ToDeletable[Key]

    class ToDeletable[Key] {
      def constant: Deletable[Key] =
        Deletable(Function.const(q) _)

      def parameters(toParameters: Key => Parameters): Deletable[Key] =
        Deletable(key => q.onParameters(toParameters(key)))

      def product[
        Repr <: HList,
        HMapKey <: Symbol,
        AsParameters <: HList
      ](implicit p: Parameters.Products[Key, Repr, HMapKey, AsParameters]
      ): Deletable[Key] =
        parameters(Parameters.product(_))

      def record[
        Repr <: HList,
        HMapKey <: Symbol,
        AsParameters <: HList
      ](implicit p: Parameters.Records[Repr, HMapKey, AsParameters],
        ev: Repr =:= Key
      ): Deletable[Key] =
        parameters(key => Parameters.record(key.asInstanceOf[Repr]))
    }

  }

  object Delete
    extends Logger {

    override protected def logClass: Class[_] = classOf[com.rocketfuel.sdbc.base.jdbc.Delete]

    def delete(
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
                Stream.eval(async.delay(delete(statement, params)))
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

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Parameters
    ): Unit = {
      log.debug(s"""query "${compiledStatement.originalQueryText}", parameters $parameters""")
    }
  }

}
