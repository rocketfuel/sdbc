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

  case class Query[T] private [cassandra] (
    override val statement: CompiledStatement,
    override val queryOptions: QueryOptions,
    override val parameterValues: Map[String, ParameterValue]
  )(implicit val converter: RowConverter[T]
  ) extends ParameterizedQuery[Query[T]]
    with HasQueryOptions
    with Logging {
    query =>

    override protected def subclassConstructor(parameterValues: Map[String, ParameterValue]): Query[T] = {
      copy(parameterValues = parameterValues)
    }

    private def bind(
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

      private def prepare(parameters: Map[String, ParameterValue])(implicit session: Session): core.BoundStatement = {
        val prepared = session.prepare(query.queryText)

        bind(prepared, parameters)
      }

      def execute(parameters: (String, ParameterValue)*)(implicit session: Session): core.ResultSet = {
        execute(parameters: Parameters)
      }

      def execute(parameters: Map[String, ParameterValue])(implicit session: Session): core.ResultSet = {
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
        logExecution(parameters)

        val prepared = prepare(parameters)
        session.execute(prepared)
      }

      def iterator(parameters: (String, ParameterValue)*)(implicit session: Session): Iterator[T] = {
        iterator(parameters: Parameters)
      }

      def iterator(parameters: Map[String, ParameterValue])(implicit session: Session): Iterator[T] = {
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
        results.fet
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

  object Query {

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
