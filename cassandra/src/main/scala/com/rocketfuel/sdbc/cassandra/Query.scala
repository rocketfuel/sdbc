package com.rocketfuel.sdbc.cassandra

import com.datastax.driver.core
import com.rocketfuel.sdbc.base._
import fs2._
import fs2.util.Async
import fs2.util.syntax._
import scala.collection.convert.wrapAsScala._
import scala.concurrent.{ExecutionContext, Future}
import shapeless.ops.record.{MapValues, ToMap}
import shapeless.{HList, LabelledGeneric}

trait Query {
  self: Cassandra =>

  /**
    * Represents a query that is ready to be run against a [[Session]].
    * @param statement is the text of the query. You can supply a String, and it will be converted to a
    *                  [[CompiledStatement]] by [[CompiledStatement!.apply(String)]].
    * @param parameters
    * @param queryOptions
    * @param converter
    * @tparam T
    */
  case class Query[T](
    override val statement: CompiledStatement,
    override val parameters: Parameters = Parameters.empty,
    queryOptions: QueryOptions = QueryOptions.default
  )(implicit val converter: RowConverter[T]
  ) extends ParameterizedQuery[Query[T]] {
    query =>

    override protected def subclassConstructor(parameters: Parameters): Query[T] = {
      copy(parameters = parameters)
    }

    def execute()(implicit session: Session): core.ResultSet = {
      Query.execute(statement, parameters, queryOptions)
    }

    def iterator()(implicit session: Session): Iterator[T] = {
      Query.iterator(statement, parameters, queryOptions)
    }

    def option()(implicit session: Session): Option[T] = {
      Query.option(statement, parameters, queryOptions)
    }

    def singleton()(implicit session: Session): T = {
      Query.singleton(statement, parameters, queryOptions)
    }

    def stream[F[_]](implicit session: Session, async: Async[F]): Stream[F, T] = {
      Query.stream[F, T](statement, parameters, queryOptions)
    }

    object future {

      def execute()(implicit session: Session, ec: ExecutionContext): Future[core.ResultSet] = {
        Query.future.execute(statement, queryOptions, parameters)
      }

      def iterator()(implicit session: Session, ec: ExecutionContext): Future[Iterator[T]] = {
        Query.future.iterator(statement, queryOptions, parameters)
      }

      def option()(implicit session: Session, ec: ExecutionContext): Future[Option[T]] = {
        Query.future.option(statement, queryOptions, parameters)
      }

    }

    object async {

      def execute[F[_]]()(implicit session: Session, async: Async[F]): F[core.ResultSet] = {
        Query.async.execute(statement, queryOptions, parameters)
      }

      def iterator[F[_]]()(implicit session: Session, async: Async[F]): F[Iterator[T]] = {
        Query.async.iterator(statement, queryOptions, parameters)
      }

      def option[F[_]]()(implicit session: Session, async: Async[F]): F[Option[T]] = {
        Query.async.option(statement, queryOptions, parameters)
      }

    }

    object task {

      def execute()(implicit session: Session, strategy: Strategy): Task[core.ResultSet] = {
        async.execute[Task]()
      }

      def iterator(implicit session: Session, strategy: Strategy): Task[Iterator[T]] = {
        async.iterator[Task]()
      }

      def option()(implicit session: Session, strategy: Strategy): Task[Option[T]] = {
        async.option[Task]()
      }

    }

    def pipe[F[_]](implicit async: Async[F]): Query.Pipe[F, T] =
      new Query.Pipe[F, T](statement, parameters, queryOptions)

    def sink[F[_]](implicit async: Async[F]): Query.Sink[F] =
      new Query.Sink[F](statement, parameters, queryOptions)

  }

  object Query
    extends QueryCompanionOps
      with Logger {

    def execute(
      statement: CompiledStatement,
      parameters: Parameters = Parameters.empty,
      queryOptions: QueryOptions = QueryOptions.default
    )(implicit session: Session
    ): core.ResultSet = {
      logExecution(statement, parameters)

      val prepared = prepare(statement, parameters, queryOptions)
      session.execute(prepared)
    }

    def iterator[A](
      statement: CompiledStatement,
      parameters: Parameters = Parameters.empty,
      queryOptions: QueryOptions = QueryOptions.default
    )(implicit converter: RowConverter[A],
      session: Session
    ): Iterator[A] = {
      execute(statement, parameters, queryOptions).iterator().map(converter)
    }

    def stream[F[_], A](
      statement: CompiledStatement,
      parameters: Parameters = Parameters.empty,
      queryOptions: QueryOptions = QueryOptions.default
    )(implicit converter: RowConverter[A],
      session: Session,
      async: Async[F]
    ): Stream[F, A] = {

      def currentChunk(results: core.ResultSet, chunkSize: Int): Stream[F, A] = {
        val chunk = Vector.fill[A](chunkSize)(results.one())
        Stream.chunk(Chunk.seq(chunk))
      }

      def chunks(results: core.ResultSet): Stream[F, A] = {
        (results.getAvailableWithoutFetching, results.isFullyFetched) match {
          case (0, false) =>
            for {
              nextResults <- Stream.eval(toAsync[F, core.ResultSet](results.fetchMoreResults()))
              one <- chunks(nextResults)
            } yield one

          case (0, true) =>
            Stream.empty[F, A]

          case (chunkSize, _) =>
            //Get results that won't cause any IO, then append the ones that do.
            currentChunk(results, chunkSize) ++ chunks(results)
        }
      }

      for {
        results <- Stream.eval(this.async[F].execute(statement, queryOptions, parameters))
        next <- chunks(results)
      } yield next
    }

    def option[A](
      statement: CompiledStatement,
      parameters: Parameters = Parameters.empty,
      queryOptions: QueryOptions = QueryOptions.default
    )(implicit converter: RowConverter[A],
      session: Session
    ): Option[A] = {
      Option(execute(statement, parameters, queryOptions).one()).map(converter)
    }

    def singleton[A](
      statement: CompiledStatement,
      parameters: Parameters = Parameters.empty,
      queryOptions: QueryOptions = QueryOptions.default
    )(implicit converter: RowConverter[A],
      session: Session
    ): A = {
      Option(execute(statement, parameters, queryOptions).one()).map(converter).
        getOrElse(throw new NoSuchElementException("empty ResultSet"))
    }

    abstract class PipeAux[F[_], A] private[Query] (
      statement: CompiledStatement,
      defaultParameters: Parameters,
      queryOptions: QueryOptions
    )(implicit async: Async[F]
    ) {
      protected def aux(additionalParameters: Parameters)(implicit session: Session): A

      def parameters(implicit session: Session): fs2.Pipe[F, Parameters, A] = {
        fs2.pipe.lift[F, Parameters, A] { parameters =>
          aux(parameters)
        }
      }

      def product[
        B,
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit session: Session,
        genericA: LabelledGeneric.Aux[B, Repr],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, AsParameters],
        toMap: ToMap.Aux[AsParameters, Key, ParameterValue]
      ): fs2.Pipe[F, B, A] = {
        fs2.pipe.lift[F, B, A] { parameters =>
          aux(Parameters.product(parameters))
        }
      }

      def record[
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit session: Session,
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, AsParameters],
        toMap: ToMap.Aux[AsParameters, Key, ParameterValue]
      ): fs2.Pipe[F, Repr, A] = {
        fs2.pipe.lift[F, Repr, A] { parameters =>
          aux(Parameters.record(parameters))
        }
      }
    }

    case class Pipe[F[_], A](
      statement: CompiledStatement,
      defaultParameters: Parameters = Parameters.empty,
      queryOptions: QueryOptions = QueryOptions.default
    )(implicit async: Async[F],
      converter: RowConverter[A]
    ) extends PipeAux[F, Stream[F, A]](
      statement,
      defaultParameters,
      queryOptions
    ) {
      override protected def aux(additionalParameters: Parameters)(implicit session: Session): Stream[F, A] = {
        stream[F, A](statement, defaultParameters ++ additionalParameters, queryOptions)
      }
    }

    case class Sink[F[_]](
      statement: CompiledStatement,
      defaultParameters: Parameters = Parameters.empty,
      queryOptions: QueryOptions = QueryOptions.default
    )(implicit async: Async[F]
    ) extends PipeAux[F, Unit](
      statement,
      defaultParameters,
      queryOptions
    ) {
      override protected def aux(additionalParameters: Parameters)(implicit session: Session): Unit = {
        execute(statement, defaultParameters ++ additionalParameters, queryOptions)
      }
    }

}

  //This class exists merely to solve the name clash that occurs
  //when a class and its companion object have objects with the same
  //name.
  trait QueryCompanionOps {
    self: Logger =>

    protected def prepare(
      statement: CompiledStatement,
      parameters: Parameters,
      queryOptions: QueryOptions
    )(implicit session: Session
    ): core.BoundStatement = {
      val prepared = session.prepare(statement.queryText)

      val forBinding = prepared.bind()

      for ((parameterName, parameterIndices) <- statement.parameterPositions) {
        val parameterValue = parameters(parameterName)
        for (parameterIndex <- parameterIndices) {
          parameterValue.set(forBinding, parameterIndex)
        }
      }

      queryOptions.set(forBinding)

      forBinding
    }

    protected def logExecution(statement: CompiledStatement, parameters: Parameters): Unit =
      log.debug(s"""Executing "${statement.originalQueryText}" with parameters $parameters.""")

    class AsyncMethods[F[_]] private[Query] (implicit async: Async[F]) {

      def execute(
        statement: CompiledStatement,
        queryOptions: QueryOptions,
        parameters: Parameters
      )(implicit session: Session
      ): F[core.ResultSet] = {
        logExecution(statement, parameters)

        val prepared = prepare(statement, parameters, queryOptions)
        toAsync[F, core.ResultSet](session.executeAsync(prepared))
      }

      def iterator[A](
        statement: CompiledStatement,
        queryOptions: QueryOptions,
        parameters: Parameters
      )(implicit converter: RowConverter[A],
        session: Session
      ): F[Iterator[A]] = {
        for {
          results <- execute(statement, queryOptions, parameters)
        } yield results.iterator().map(converter)
      }

      def option[A](
        statement: CompiledStatement,
        queryOptions: QueryOptions,
        parameters: Parameters
      )(implicit converter: RowConverter[A],
        session: Session
      ): F[Option[A]] = {
        for {
          results <- execute(statement, queryOptions, parameters)
        } yield Option(results.one()).map(converter)
      }

    }

    def async[F[_]](implicit async: Async[F]): AsyncMethods[F] =
      new AsyncMethods[F]

    def task(implicit strategy: Strategy): AsyncMethods[Task] =
      new AsyncMethods[Task]

    object future {

      def execute(
        statement: CompiledStatement,
        queryOptions: QueryOptions,
        parameters: Parameters
      )(implicit session: Session,
        ec: ExecutionContext
      ): Future[core.ResultSet] = {
        logExecution(statement, parameters)

        val prepared = prepare(statement, parameters, queryOptions)
        toScalaFuture(session.executeAsync(prepared))
      }

      def iterator[A](
        statement: CompiledStatement,
        queryOptions: QueryOptions,
        parameters: Parameters
      )(implicit converter: RowConverter[A],
        session: Session,
        ec: ExecutionContext
      ): Future[Iterator[A]] = {
        execute(statement, queryOptions, parameters).map(_.iterator().map(converter))
      }

      def option[A](
        statement: CompiledStatement,
        queryOptions: QueryOptions,
        parameters: Parameters
      )(implicit converter: RowConverter[A],
        session: Session,
        ec: ExecutionContext
      ): Future[Option[A]] = {
        for {
          results <- execute(statement, queryOptions, parameters)
        } yield Option(results.one()).map(converter)
      }

    }
  }

}
