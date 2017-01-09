package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logger
import fs2.{Stream, pipe}
import fs2.util.Async
import shapeless.HList

trait Ignore {
  self: DBMS with Connection =>

  trait IgnorableQuery[Self <: IgnorableQuery[Self]]
    extends CompiledParameterizedQuery[Self] {

    def ignore()(implicit connection: Connection): Unit = {
      Ignore.ignore(statement, parameters)
    }

    def sink[F[_]](implicit async: Async[F]): Ignore.Sink[F] =
      Ignore.Sink[F](statement, parameters)

  }

  case class Ignore(
    override val statement: CompiledStatement,
    override val parameters: Parameters = Parameters.empty
  ) extends IgnorableQuery[Ignore] {
    q =>

    override def subclassConstructor(parameters: Parameters): Ignore = {
      copy(parameters = parameters)
    }

    /**
      * Get helper methods for creating [[Ignorable]]s from this query.
      */
    def ignorable[Key]: ToIgnorable[Key] =
      new ToIgnorable[Key]

    class ToIgnorable[Key] {
      def constant: Ignorable[Key] =
        Function.const(q) _

      def parameters(toParameters: Key => Parameters): Ignorable[Key] =
        key => q.onParameters(toParameters(key))

      def product[
        Repr <: HList,
        HMapKey <: Symbol,
        AsParameters <: HList
      ](implicit p: Parameters.Products[Key, Repr, HMapKey, AsParameters]
      ): Ignorable[Key] =
        parameters(Parameters.product(_))

      def record[
        Repr <: HList,
        HMapKey <: Symbol,
        AsParameters <: HList
      ](implicit p: Parameters.Records[Repr, HMapKey, AsParameters],
        ev: Repr =:= Key
      ): Ignorable[Key] =
        parameters(key => Parameters.record(key.asInstanceOf[Repr]))
    }

  }

  object Ignore
    extends QueryCompanion[Ignore] {

    override protected def logClass: Class[_] = classOf[com.rocketfuel.sdbc.base.jdbc.Ignore]

    override protected def ofCompiledStatement(statement: CompiledStatement): Ignore =
      Ignore(statement)

    def ignore(
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
            StreamUtils.connection {implicit connection =>
              Stream.eval(async.delay(ignore(statement, params)))
            }
          }
        )
      }

      def products[
        A,
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        p: Parameters.Products[A, Repr, Key, AsParameters]
      ): fs2.Sink[F, A] = {
        parameterPipe.products.andThen(parameters)
      }

      def records[
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        r: Parameters.Records[Repr, Key, AsParameters]
      ): fs2.Sink[F, Repr] = {
        parameterPipe.records.andThen(parameters)
      }
    }

    implicit val partable: Batch.Partable[Ignore] =
      (q: Ignore) => Batch.Part(q.statement, q.parameters)

  }

}
