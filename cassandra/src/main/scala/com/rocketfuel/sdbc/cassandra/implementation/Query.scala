package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core
import com.rocketfuel.sdbc.base.CompiledStatement
import com.rocketfuel.sdbc.cassandra._
import fs2.util.Async
import fs2.{Pipe, Sink, Strategy, Stream, Task}
import scala.collection.convert.wrapAsScala._
import scala.concurrent.{ExecutionContext, Future}
import shapeless.ops.hlist._
import shapeless.ops.record.{Keys, Values}
import shapeless.{HList, LabelledGeneric}

trait Query {
  self: Cassandra =>

  case class Query[T] private [cassandra] (
    override val statement: CompiledStatement,
    override val queryOptions: QueryOptions,
    override val parameters: Parameters
  )(implicit val converter: RowConverter[T]
  ) extends ParameterizedQuery[Query[T]]
    with HasQueryOptions {
    query =>

    override protected def subclassConstructor(parameters: Parameters): Query[T] = {
      copy(parameters = parameters)
    }

    private def prepare()(implicit session: Session): core.BoundStatement = {
      val prepared = session.prepare(query.queryText)

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

    def execute()(implicit session: Session): core.ResultSet = {
      logExecution()

      val prepared = prepare()
      session.execute(prepared)
    }

    def iterator()(implicit session: Session): Iterator[T] = {
      val results = execute()
      results.iterator().map(converter)
    }

    def option()(implicit session: Session): Option[T] = {
      val results = execute()
      Option(results.one()).map(converter)
    }

    object future {

      def execute()(implicit session: Session, ec: ExecutionContext): Future[core.ResultSet] = {
        logExecution()

        val prepared = prepare()
        toScalaFuture(session.executeAsync(prepared))
      }

      def iterator()(implicit session: Session, ec: ExecutionContext): Future[Iterator[T]] = {
        execute().map(_.iterator().map(converter))
      }

      def option()(implicit session: Session, ec: ExecutionContext): Future[Option[T]] = {
        for {
          results <- execute()
        } yield Option(results.one()).map(converter)
      }

    }

    object task {

      def execute()(implicit session: Session, strategy: Strategy): Task[core.ResultSet] = {
        logExecution()

        val prepared = prepare()
        toAsync[Task, core.ResultSet](session.executeAsync(prepared))
      }

      def iterator()(implicit session: Session, strategy: Strategy): Task[Iterator[T]] = {
        execute().map(_.iterator().map(converter))
      }

      def option()(implicit session: Session, strategy: Strategy): Task[Option[T]] = {
        for {
          results <- execute()
        } yield Option(results.one()).map(converter)
      }

    }

    def pipe[F[_]](implicit
      session: Session,
      rowConverter: RowConverter[T],
      async: Async[F]
    ): PipeOps[F] =
      new PipeOps[F]

    def sink[F[_]](implicit
      session: Session,
      rowConverter: RowConverter[T],
      async: Async[F]
    ): SinkOps[F] =
      new SinkOps[F]

    class PipeOps[F[_]] private[Query] {
      def parameters(implicit
        session: Session,
        rowConverter: RowConverter[T],
        async: Async[F]
      ): Pipe[F, Parameters, Stream[F, T]] = {
        fs2.pipe.lift[F, Parameters, Stream[F, T]] { parameters =>
          Query.iteratorToStream(onParameters(parameters).iterator())
        }
      }

      def product[
        A,
        Repr <: HList,
        ReprKeys <: HList,
        ReprValues <: HList,
        MappedRepr <: HList
      ](implicit
        session: Session,
        rowConverter: RowConverter[T],
        async: Async[F],
        genericA: LabelledGeneric.Aux[A, Repr],
        keys: Keys.Aux[Repr, ReprKeys],
        values: Values.Aux[Repr, ReprValues],
        valuesMapper: Mapper.Aux[ToParameterValue.type, ReprValues, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Pipe[F, A, Stream[F, T]] = {
        fs2.pipe.lift[F, A, Stream[F, T]] { parameters =>
          Query.iteratorToStream(onProduct(parameters).iterator())
        }
      }

      def record[
        Repr <: HList,
        ReprKeys <: HList,
        ReprValues <: HList,
        MappedRepr <: HList
      ](implicit session: Session,
        rowConverter: RowConverter[T],
        async: Async[F],
        keys: Keys.Aux[Repr, ReprKeys],
        values: Values.Aux[Repr, ReprValues],
        valuesMapper: Mapper.Aux[ToParameterValue.type, ReprValues, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Pipe[F, Repr, Stream[F, T]] = {
        fs2.pipe.lift[F, Repr, Stream[F, T]] { parameters =>
          Query.iteratorToStream(onRecord(parameters).iterator())
        }
      }
    }

    class SinkOps[F[_]] private[Query] {
      def parameters(implicit
        session: Session,
        rowConverter: RowConverter[T],
        async: Async[F]
      ): Sink[F, Parameters] = {
        fs2.pipe.lift[F, Parameters, Unit] { parameters =>
          onParameters(parameters).execute()
        }
      }

      def product[
        A,
        Repr <: HList,
        ReprKeys <: HList,
        ReprValues <: HList,
        MappedRepr <: HList
      ](implicit
        session: Session,
        rowConverter: RowConverter[T],
        async: Async[F],
        genericA: LabelledGeneric.Aux[A, Repr],
        keys: Keys.Aux[Repr, ReprKeys],
        values: Values.Aux[Repr, ReprValues],
        valuesMapper: Mapper.Aux[ToParameterValue.type, ReprValues, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Sink[F, A] = {
        fs2.pipe.lift[F, A, Unit] { parameters =>
          onProduct(parameters).execute()
        }
      }

      def record[
        Repr <: HList,
        ReprKeys <: HList,
        ReprValues <: HList,
        MappedRepr <: HList
      ](implicit session: Session,
        rowConverter: RowConverter[T],
        async: Async[F],
        keys: Keys.Aux[Repr, ReprKeys],
        values: Values.Aux[Repr, ReprValues],
        valuesMapper: Mapper.Aux[ToParameterValue.type, ReprValues, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Sink[F, Repr] = {
        fs2.pipe.lift[F, Repr, Unit] { parameters =>
          onRecord(parameters).execute()
        }
      }
    }

  }

  object Query {

    def apply[T](
      queryText: String,
      hasParameters: Boolean = true,
      queryOptions: QueryOptions = QueryOptions.default
    )(implicit converter: RowConverter[T]
    ): Query[T] = {
      Query[T](
        if (hasParameters) CompiledStatement(queryText) else CompiledStatement.literal(queryText),
        queryOptions,
        Parameters.empty
      )
    }

    private[implementation] def iteratorToStream[F[_], A](i: Iterator[A])(implicit a: Async[F]): Stream[F, A] = {
      val step: F[Option[A]] = a.delay {
        if (i.hasNext) Some(i.next)
        else None
      }

      Stream.eval(step).repeat.through(fs2.pipe.unNoneTerminate)
    }

    private[implementation] def iteratorToStream[F[_], A](i: F[Iterator[A]])(implicit a: Async[F]): Stream[F, A] = {
      for {
        iterator <- Stream.eval(i)
        elem <- iteratorToStream(iterator)
      } yield elem
    }

  }

}
