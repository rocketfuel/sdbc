package com.rocketfuel.sdbc.base.jdbc

import java.io.Closeable
import java.lang.Process
import java.sql._
import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.{Logging, CompiledStatement}
import scala.concurrent.{Future, ExecutionContext}
import scalaz.concurrent.Task
import scalaz.stream._
import shapeless.ops.hlist._
import shapeless.ops.record.{MapValues, Keys}
import shapeless.{LabelledGeneric, HList}

trait Query {
  self: DBMS =>

  abstract class Query[R <: Row, T] private[jdbc](
    override val statement: CompiledStatement,
    override val parameterValues: Map[String, ParameterValue]
  )(implicit val converter: RowConverter[R, T]
  ) extends ParameterizedQuery[Query[R, T]]
  with Logging {

    private def prepareStatement()(implicit connection: Connection): PreparedStatement = {
      connection.prepareStatement(statement.queryText)
    }

    private def bind(
      preparedStatement: PreparedStatement,
      parameterValues: Map[String, ParameterValue]
    ): PreparedStatement = {
      for ((parameterName, parameterIndices) <- statement.parameterPositions) {
        val parameterValue = parameterValues(parameterName)
        for (parameterIndex <- parameterIndices) {
          parameterValue.set(preparedStatement, parameterIndex)
        }
      }

      preparedStatement
    }

    private def executeStatement(
      extraParameters: Parameters
    )(implicit connection: Connection): PreparedStatement = {
      val withExtraParameters = setParameters(extraParameters)

      logExecution(withExtraParameters)

      val prepared = prepareStatement()
      val bound = bind(prepared, withExtraParameters)

      bound.execute()
      bound
    }

    object io {

      def execute(parameters: (String, ParameterValue)*)(implicit connection: Connection): Statement = {
        executeStatement(parameters: Parameters)
      }

      def execute(parameters: Map[String, ParameterValue])(implicit connection: Connection): Statement = {
        executeStatement(parameters: Parameters)
      }

      def execute[
        A,
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: A
      )(implicit connection: Connection,
        genericA: LabelledGeneric.Aux[A, Repr],
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Statement = {
        executeStatement(parameters: Parameters)
      }

      def execute[
        Repr <: HList,
        ReprKeys <: HList,
        MappedRepr <: HList
      ](parameters: Repr
      )(implicit connection: Connection,
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Statement = {
        executeStatement(parameters: Parameters).getMoreResults
      }

      def iterator(parameters: (String, ParameterValue)*)(implicit connection: Connection): Iterator[T] = {
        executeStatement()
      }

      def iterator(parameters: Map[String, ParameterValue])(implicit connection: Connection): Iterator[T] = {
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
      ): Iterator[T] = {
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
      ): Iterator[T] = {
        iterator(parameters: Parameters)
      }

      private[implementation] def iterator(additionalParameters: Parameters)(implicit session: Session): Iterator[T] = {
        val results = execute(additionalParameters)
        Row.iterator(results).map(converter)
      }

      def option(parameters: (String, ParameterValue)*)(implicit session: Session): Option[T] = {
        option(parameters: Parameters)
      }

      def option(parameters: Map[String, ParameterValue])(implicit session: Session): Option[T] = {
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
      ): Option[T] = {
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
      ): Option[T] = {
        option(parameters: Parameters)
      }

      private[implementation] def option(additionalParameters: Parameters)(implicit session: Session): Option[T] = {
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
        val parameters = setParameters(additionalParameters)
        logExecution(parameters)

        for {
          prepared <- prepare(parameters)
          result <- implementation.toScalaFuture(session.executeAsync(prepared))
        } yield result
      }

      def iterator(parameters: (String, ParameterValue)*)(implicit session: Session, ec: ExecutionContext): Future[Iterator[T]] = {
        iterator(parameters: Parameters)
      }

      def iterator(parameters: Map[String, ParameterValue])(implicit session: Session, ec: ExecutionContext): Future[Iterator[T]] = {
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
      ): Future[Iterator[T]] = {
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
      ): Future[Iterator[T]] = {
        iterator(parameters: Parameters)
      }

      private[implementation] def iterator(parameterValues: Parameters)(implicit session: Session, ec: ExecutionContext): Future[Iterator[T]] = {
        for {
          result <- execute(parameterValues)
        } yield {
          Row.iterator(result).map(convertRow)
        }
      }

      def option(parameters: (String, ParameterValue)*)(implicit session: Session, ec: ExecutionContext): Future[Option[T]] = {
        option(parameters: Parameters)
      }

      def option(parameters: Map[String, ParameterValue])(implicit session: Session, ec: ExecutionContext): Future[Option[T]] = {
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
      ): Future[Option[T]] = {
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
      ): Future[Option[T]] = {
        option(parameters: Parameters)
      }

      private[implementation] def option(parameterValues: Parameters)(implicit session: Session, ec: ExecutionContext): Future[Option[T]] = {
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
        logExecution(parameters)

        for {
          prepared <- prepare(parameters)
          result <- toTask(session.executeAsync(prepared))
        } yield result
      }

      def iterator(parameters: (String, ParameterValue)*)(implicit session: Session): Task[Iterator[T]] = {
        iterator(parameters: Parameters)
      }

      def iterator(parameters: Map[String, ParameterValue])(implicit session: Session): Task[Iterator[T]] = {
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
      ): Task[Iterator[T]] = {
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
      ): Task[Iterator[T]] = {
        iterator(parameters: Parameters)
      }

      private[implementation] def iterator(additionalParameters: Parameters)(implicit session: Session): Task[Iterator[T]] = {
        for {
          result <- execute(additionalParameters)
        } yield {
          Row.iterator(result).map(converter)
        }
      }

      def option(parameters: (String, ParameterValue)*)(implicit session: Session): Task[Option[T]] = {
        option(parameters: Parameters)
      }

      def option(parameters: Map[String, ParameterValue])(implicit session: Session): Task[Option[T]] = {
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
      ): Task[Option[T]] = {
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
      ): Task[Option[T]] = {
        option(parameters: Parameters)
      }

      private def option(additionalParameters: Parameters)(implicit session: Session): Task[Option[T]] = {
        for {
          result <- execute(additionalParameters)
        } yield {
          Option(result.one()).map(convertRow)
        }
      }

    }

    def stream(parameters: (String, ParameterValue)*)(implicit session: Session): Process[Task, T] = {
      stream(parameters: Parameters)
    }

    def stream(parameters: Map[String, ParameterValue])(implicit session: Session): Process[Task, T] = {
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
    ): Process[Task, T] = {
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
    ): Process[Task, T] = {
      stream(parameters: Parameters)
    }

    private[implementation] def stream(additionalParameters: Parameters)(implicit session: Session): Process[Task, T] = {
      val parameters = setParameters(additionalParameters)
      logExecution(parameters)

      Process.await(task.prepare(parameters)) { bound =>
        val iterator = for {
          result <- toTask(session.executeAsync(bound))
        } yield {
          Row.iterator(result).map(convertRow)
        }

        scalaz.stream.io.iterator(iterator)
      }
    }

  }

  object Query {

    def updatable[T](
      queryText: String,
      hasParameters: Boolean = true
    )(implicit converter: RowConverter[UpdatableRow, T]
    ): Query[UpdatableRow, T] = {
      Query[T](
        CompiledStatement(queryText, hasParameters),
        Map.empty[String, ParameterValue]
      )
    }

    object stream {
      def ofQueries[T](implicit cluster: core.Cluster): Channel[Task, Query[T], Process[Task, T]] = {
        val req = toTask(cluster.connectAsync())
        def release(session: Session): Task[Unit] = {
          toTask(session.closeAsync()).map(Function.const(()))
        }
        channel.lift[Task, Query[T], Process[Task, T]] { query =>
          Task.delay {
            scalaz.stream.io.iteratorR[Session, T](req)(release) {implicit session =>
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
