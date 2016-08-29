package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core
import com.rocketfuel.sdbc.base.CompiledStatement
import com.rocketfuel.sdbc.cassandra._
import scala.collection.convert.wrapAsScala._
import scala.concurrent.{ExecutionContext, Future}
import scalaz.concurrent.Task
import scalaz.stream._
import shapeless.ops.hlist._
import shapeless.ops.record.{Keys, MapValues}
import shapeless.{HList, LabelledGeneric}

trait Query {
  self: Cassandra =>

  case class Query[T] private [cassandra] (
    override val statement: CompiledStatement,
    override val queryOptions: QueryOptions,
    override val parameterValues: Map[String, ParameterValue]
  )(implicit val converter: RowConverter[T]
  ) extends ParameterizedQuery[Query[T]]
    with HasQueryOptions {
    query =>

    override protected def subclassConstructor(parameterValues: Map[String, ParameterValue]): Query[T] = {
      copy(parameterValues = parameterValues)
    }

    private def prepare()(implicit session: Session): core.BoundStatement = {
      val prepared = session.prepare(query.queryText)

      val forBinding = prepared.bind()

      for ((parameterName, parameterIndices) <- statement.parameterPositions) {
        val parameterValue = parameterValues(parameterName)
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

      def execute()(implicit session: Session): Task[core.ResultSet] = {
        logExecution()

        val prepared = prepare()
        toTask(session.executeAsync(prepared))
      }

      def iterator()(implicit session: Session): Task[Iterator[T]] = {
        execute().map(_.iterator().map(converter))
      }

      def option()(implicit session: Session): Task[Option[T]] = {
        for {
          results <- execute()
        } yield Option(results.one()).map(converter)
      }

    }

    def channel(implicit
      session: Session,
      rowConverter: RowConverter[T]
    ): Channel[Task, Parameters, Process[Task, T]] = {
      scalaz.stream.channel.lift[Task, Parameters, Process[Task, T]] { parameters =>
        Task(io.iterator(onParameters(parameters.parameters).task.iterator()))
      }
    }

    def productChannel[
      P,
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](implicit session: Session,
      rowConverter: RowConverter[T],
      genericA: LabelledGeneric.Aux[P, Repr],
      keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Channel[Task, P, Process[Task, T]] = {
      scalaz.stream.channel.lift[Task, P, Process[Task, T]] {
        product => Task(io.iterator(Task(onProduct(product).iterator())))
      }
    }

    def recordChannel[
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](implicit session: Session,
      rowConverter: RowConverter[T],
      keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Channel[Task, Repr, Process[Task, T]] = {
      scalaz.stream.channel.lift[Task, Repr, Process[Task, T]] {
        product => Task(io.iterator(Task(onRecord(product).iterator())))
      }
    }

    def sink(implicit
      session: Session,
      rowConverter: RowConverter[T]
    ): Sink[Task, Parameters] = {
      scalaz.stream.sink.lift[Task, Parameters] { parameters =>
        Task[Unit] {
          onParameters(parameters.parameters).task.execute()
        }
      }
    }

    def productSink[
      P,
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](implicit session: Session,
      rowConverter: RowConverter[T],
      genericA: LabelledGeneric.Aux[P, Repr],
      keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Sink[Task, P] = {
      scalaz.stream.sink.lift[Task, P] {
        product => Task(onProduct(product).execute())
      }
    }

    def recordSink[
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](implicit session: Session,
      rowConverter: RowConverter[T],
      keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Sink[Task, Repr] = {
      scalaz.stream.sink.lift[Task, Repr] {
        product => Task(onRecord(product).execute())
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
        Map.empty[String, ParameterValue]
      )
    }

  }

}
