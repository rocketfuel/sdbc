package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core
import com.rocketfuel.sdbc.base.{CompiledStatement, Logging}
import com.rocketfuel.sdbc.cassandra._
import scala.concurrent.{ExecutionContext, Future}
import scalaz.concurrent.Task
import scalaz.stream._
import shapeless.ops.hlist._
import shapeless.ops.record.{Keys, MapValues}
import shapeless.{HList, LabelledGeneric}

trait Query {
  self: Cassandra =>

  case class Query[A] private [cassandra] (
    override val statement: CompiledStatement,
    override val queryOptions: QueryOptions,
    override val parameterValues: Map[String, ParameterValue]
  )(implicit val resultSetConverter: ResultSetConverter[A]
  ) extends ParameterizedQuery[Query[A]]
    with HasQueryOptions {
    query =>

    private def logRun(
      parameters: Map[String, ParameterValue]
    ): Unit = {
      Query.logRun(statement, parameters)
    }

    override protected def subclassConstructor(parameterValues: Map[String, ParameterValue]): Query[A] = {
      copy(parameterValues = parameterValues)
    }

    object io {

      private def prepare(parameters: Map[String, ParameterValue])(implicit session: Session): core.BoundStatement = {
        val prepared = session.prepare(query.queryText)

        Query.bind(statement, queryOptions, prepared, parameters)
      }

      def execute(parameters: (String, ParameterValue)*)(implicit session: Session): core.ResultSet = {
        execute(parameters: Parameters)
      }

      def execute(parameters: Map[String, ParameterValue])(implicit session: Session): core.ResultSet = {
        execute(parameters: Parameters)
      }

      def execute[
        P,
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: P
      )(implicit session: Session,
        genericA: LabelledGeneric.Aux[P, Repr],
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): core.ResultSet = {
        execute(parameters: Parameters)
      }

      def execute[
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: Repr
      )(implicit session: Session,
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): core.ResultSet = {
        execute(parameters: Parameters)
      }

      private[implementation] def execute(additionalParameters: Parameters)(implicit session: Session): core.ResultSet = {
        val parameters = setParameters(additionalParameters.parameters)
        logRun(parameters)

        val prepared = prepare(parameters)
        session.execute(prepared)
      }

      def iterator(parameters: (String, ParameterValue)*)(implicit session: Session): Iterator[A] = {
        iterator(parameters: Parameters)
      }

      def iterator(parameters: Map[String, ParameterValue])(implicit session: Session): Iterator[A] = {
        iterator(parameters: Parameters)
      }

      def iterator[
        P,
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: P
      )(implicit session: Session,
        genericA: LabelledGeneric.Aux[P, Repr],
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Iterator[P] = {
        iterator(parameters: Parameters)
      }

      def iterator[
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: Repr
      )(implicit session: Session,
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Iterator[A] = {
        iterator(parameters: Parameters)
      }

      private[implementation] def iterator(additionalParameters: Parameters)(implicit session: Session): Iterator[A] = {
        val results = execute(additionalParameters)
        Row.iterator(results).map(converter)
      }

      def option(parameters: (String, ParameterValue)*)(implicit session: Session): Option[A] = {
        option(parameters: Parameters)
      }

      def option(parameters: Map[String, ParameterValue])(implicit session: Session): Option[A] = {
        option(parameters: Parameters)
      }

      def option[
        P,
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: P
      )(implicit session: Session,
        genericA: LabelledGeneric.Aux[P, Repr],
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Option[P] = {
        option(parameters: Parameters)
      }

      def option[
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: Repr
      )(implicit session: Session,
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Option[A] = {
        option(parameters: Parameters)
      }

      private[implementation] def option(additionalParameters: Parameters)(implicit session: Session): Option[A] = {
        val results = execute(additionalParameters)
        Option(results.one()).map(convertRow)
      }

    }

    object future {

      private[implementation] def prepare(parameters: Map[String, ParameterValue])(implicit session: Session, ec: ExecutionContext): Future[core.BoundStatement] = {
        for {
          prepared <- toScalaFuture(session.prepareAsync(query.queryText))
        } yield {
          bind(prepared, parameters)
        }
      }

      def execute(parameters: (String, ParameterValue)*)(implicit session: Session, ec: ExecutionContext): Future[core.ResultSet] = {
        execute(parameters: Parameters)
      }

      def execute(parameters: Map[String, ParameterValue])(implicit session: Session, ec: ExecutionContext): Future[core.ResultSet] = {
        execute(parameters: Parameters)
      }

      def execute[
        A,
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: A
      )(implicit session: Session,
        ec: ExecutionContext,
        genericA: LabelledGeneric.Aux[A, Repr],
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Future[core.ResultSet] = {
        execute(parameters: Parameters)
      }

      def execute[
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: Repr
      )(implicit session: Session,
        ec: ExecutionContext,
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Future[core.ResultSet] = {
        execute(parameters: Parameters)
      }
      
      private[implementation] def execute(additionalParameters: Parameters)(implicit session: Session, ec: ExecutionContext): Future[core.ResultSet] = {
        val parameters = setParameters(additionalParameters.parameters)
        logRun(parameters)

        for {
          prepared <- prepare(parameters)
          result <- implementation.toScalaFuture(session.executeAsync(prepared))
        } yield result
      }

      def iterator(parameters: (String, ParameterValue)*)(implicit session: Session, ec: ExecutionContext): Future[Iterator[A]] = {
        iterator(parameters: Parameters)
      }

      def iterator(parameters: Map[String, ParameterValue])(implicit session: Session, ec: ExecutionContext): Future[Iterator[A]] = {
        iterator(parameters: Parameters)
      }

      def iterator[
        A,
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: A
      )(implicit session: Session,
        ec: ExecutionContext,
        genericA: LabelledGeneric.Aux[A, Repr],
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Future[Iterator[A]] = {
        iterator(parameters: Parameters)
      }

      def iterator[
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: Repr
      )(implicit session: Session,
        ec: ExecutionContext,
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Future[Iterator[A]] = {
        iterator(parameters: Parameters)
      }
  
      private[implementation] def iterator(parameterValues: Parameters)(implicit session: Session, ec: ExecutionContext): Future[Iterator[A]] = {
        for {
          result <- execute(parameterValues)
        } yield {
          Row.iterator(result).map(convertRow)
        }
      }

      def option(parameters: (String, ParameterValue)*)(implicit session: Session, ec: ExecutionContext): Future[Option[A]] = {
        option(parameters: Parameters)
      }

      def option(parameters: Map[String, ParameterValue])(implicit session: Session, ec: ExecutionContext): Future[Option[A]] = {
        option(parameters: Parameters)
      }

      def option[
        A,
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: A
      )(implicit session: Session,
        ec: ExecutionContext,
        genericA: LabelledGeneric.Aux[A, Repr],
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Future[Option[A]] = {
        option(parameters: Parameters)
      }

      def option[
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: Repr
      )(implicit session: Session,
        ec: ExecutionContext,
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Future[Option[A]] = {
        option(parameters: Parameters)
      }

      private[implementation] def option(parameterValues: Parameters)(implicit session: Session, ec: ExecutionContext): Future[Option[A]] = {
        for {
          result <- execute(parameterValues)
        } yield {
          Option(result.one()).map(convertRow)
        }
      }

    }

    object task {

      private[Query] def prepare(parameters: Map[String, ParameterValue])(implicit session: Session): Task[core.BoundStatement] = {
        for {
          prepared <- toTask(session.prepareAsync(query.queryText))
        } yield {
          bind(prepared, parameters)
        }
      }

      def execute(parameters: (String, ParameterValue)*)(implicit session: Session): Task[core.ResultSet] = {
        execute(parameters: Parameters)
      }

      def execute(parameters: Map[String, ParameterValue])(implicit session: Session): Task[core.ResultSet] = {
        execute(parameters: Parameters)
      }

      def execute[
        A,
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: A
      )(implicit session: Session,
        genericA: LabelledGeneric.Aux[A, Repr],
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Task[core.ResultSet] = {
        execute(parameters: Parameters)
      }

      def execute[
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: Repr
      )(implicit session: Session,
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Task[core.ResultSet] = {
        execute(parameters: Parameters)
      }
      
      private[implementation] def execute(additionalParameters: Parameters)(implicit session: Session): Task[core.ResultSet] = {
        val parameters = setParameters(additionalParameters)
        logRun(parameters)

        for {
          prepared <- prepare(parameters)
          result <- toTask(session.executeAsync(prepared))
        } yield result
      }
      
      def iterator(parameters: (String, ParameterValue)*)(implicit session: Session): Task[Iterator[A]] = {
        iterator(parameters: Parameters)
      }

      def iterator(parameters: Map[String, ParameterValue])(implicit session: Session): Task[Iterator[A]] = {
        iterator(parameters: Parameters)
      }

      def iterator[
        A,
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: A
      )(implicit session: Session,
        genericA: LabelledGeneric.Aux[A, Repr],
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Task[Iterator[A]] = {
        iterator(parameters: Parameters)
      }

      def iterator[
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: Repr
      )(implicit session: Session,
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Task[Iterator[A]] = {
        iterator(parameters: Parameters)
      }

      private[implementation] def iterator(additionalParameters: Parameters)(implicit session: Session): Task[Iterator[A]] = {
        for {
          result <- execute(additionalParameters)
        } yield {
          Row.iterator(result).map(converter)
        }
      }

      def option(parameters: (String, ParameterValue)*)(implicit session: Session): Task[Option[A]] = {
        option(parameters: Parameters)
      }

      def option(parameters: Map[String, ParameterValue])(implicit session: Session): Task[Option[A]] = {
        option(parameters: Parameters)
      }

      def option[
        A,
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: A
      )(implicit session: Session,
        genericA: LabelledGeneric.Aux[A, Repr],
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Task[Option[A]] = {
        option(parameters: Parameters)
      }

      def option[
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: Repr
      )(implicit session: Session,
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Task[Option[A]] = {
        option(parameters: Parameters)
      }

      private def option(additionalParameters: Parameters)(implicit session: Session): Task[Option[A]] = {
        for {
          result <- execute(additionalParameters)
        } yield {
          Option(result.one()).map(convertRow)
        }
      }

    }

    def stream(parameters: (String, ParameterValue)*)(implicit session: Session): Process[Task, A] = {
      stream(parameters: Parameters)
    }

    def stream(parameters: Map[String, ParameterValue])(implicit session: Session): Process[Task, A] = {
      stream(parameters: Parameters)
    }

    def stream[
      A,
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](parameters: A
    )(implicit session: Session,
      genericA: LabelledGeneric.Aux[A, Repr],
      keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Process[Task, A] = {
      stream(parameters: Parameters)
    }

    def stream[
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](parameters: Repr
    )(implicit session: Session,
      keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Process[Task, A] = {
      stream(parameters: Parameters)
    }

    private[implementation] def stream(additionalParameters: Parameters)(implicit session: Session): Process[Task, A] = {
      import collection.convert.wrapAsScala._

      val parameters = setParameters(additionalParameters.parameters)
      logRun(parameters)

      Process.await(task.prepare(parameters)) { bound =>
        val iterator = for {
          result <- toTask(session.executeAsync(bound))
        } yield {
          result.iterator().map(converter)
        }

        scalaz.stream.io.iterator(iterator)
      }
    }

  }

  object Query
    extends Logging {

    def apply[T](
      queryText: String,
      hasParameters: Boolean = true,
      queryOptions: QueryOptions = QueryOptions.default
    )(implicit converter: RowConverter[T]
    ): Query[T] = {
      Query[T](
        CompiledStatement(queryText, hasParameters),
        queryOptions,
        Map.empty[String, ParameterValue]
      )
    }

    private def logRun(
      compiledStatement: CompiledStatement,
      parameters: Map[String, ParameterValue]
    ): Unit = {
      logger.debug(s"""Executing "${compiledStatement.originalQueryText}" with parameters $parameters.""")
    }

    private def bind(
      statement: CompiledStatement,
      queryOptions: QueryOptions,
      preparedStatement: core.PreparedStatement,
      parameterValues: Map[String, ParameterValue]
    ): core.BoundStatement = {
      val forBinding = preparedStatement.bind()

      for ((parameterName, parameterIndices) <- statement.parameterPositions) {
        val parameterValue = parameterValues(parameterName)
        for (parameterIndex <- parameterIndices) {
          parameterValue.set(forBinding, parameterIndex)
        }
      }

      queryOptions.set(forBinding)

      forBinding
    }

    object stream {
      def ofQueries[A](implicit cluster: core.Cluster): Channel[Task, Query[A], Process[Task, A]] = {
        val req = toTask(cluster.connectAsync())
        def release(session: Session): Task[Unit] = {
          toTask(session.closeAsync()).map(Function.const(()))
        }
        channel.lift[Task, Query[A], Process[Task, A]] { query =>
          Task.delay {
            scalaz.stream.io.iteratorR[Session, A](req)(release) {implicit session =>
              query.task.iterator()
            }
          }
        }
      }

      def ofQueriesWithKeyspace[T](implicit cluster: core.Cluster): Channel[Task, (String, Query[T]), Process[Task, T]] = {
        def release(session: Session): Task[Unit] = {
          toTask(session.closeAsync()).map(Function.const(()))
        }
        channel.lift[Task, (String, Query[T]), Process[Task, T]] {
          case (keyspace, query) =>
            val req = toTask(cluster.connectAsync(keyspace))
            Task.delay {
              scalaz.stream.io.iteratorR[Session, T](req)(release) {implicit session =>
                query.task.iterator()
              }
            }
        }
      }

      def ofParameters[T](
        queryText: String,
        hasParameters: Boolean = true,
        queryOptions: QueryOptions = QueryOptions.default
      )(implicit cluster: Cluster,
        rowConverter: RowConverter[T]
      ): Channel[Task, Parameters, Process[Task, T]] = {
        val query = Query(queryText, hasParameters, queryOptions)
        val req = toTask(cluster.connectAsync())
        def release(session: Session): Task[Unit] = {
          toTask(session.closeAsync()).map(Function.const(()))
        }
        channel.lift[Task, Parameters, Process[Task, T]] { parameters =>
          Task.delay {
            scalaz.stream.io.iteratorR[Session, T](req)(release) {implicit session =>
              query.task.iterator(parameters)
            }
          }
        }
      }

      def ofParametersWithKeyspace[T](
        queryText: String,
        hasParameters: Boolean = true,
        queryOptions: QueryOptions = QueryOptions.default
      )(implicit cluster: Cluster,
        rowConverter: RowConverter[T]
      ): Channel[Task, (String, Parameters), Process[Task, T]] = {
        val query = Query(queryText, hasParameters, queryOptions)
        def release(session: Session): Task[Unit] = {
          toTask(session.closeAsync()).map(Function.const(()))
        }
        channel.lift[Task, (String, Parameters), Process[Task, T]] {
          case (keyspace, parameters) =>
            val req = toTask(cluster.connectAsync(keyspace))
            Task.delay {
              scalaz.stream.io.iteratorR[Session, T](req)(release) {implicit session =>
                query.task.iterator(parameters)
              }
            }
        }
      }
    }
  }

}
