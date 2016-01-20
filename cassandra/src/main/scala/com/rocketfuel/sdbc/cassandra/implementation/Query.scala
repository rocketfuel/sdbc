package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core
import com.rocketfuel.sdbc.base.{CompiledStatement, Logging}
import com.rocketfuel.sdbc.cassandra._
import scala.collection.convert.decorateAsScala._
import scala.concurrent.{ExecutionContext, Future}
import scalaz.concurrent.Task
import scalaz.stream._

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

    private def convertRow(row: core.Row): T = {
      converter(Row(row))
    }

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

    private def logExecution(parameters: Map[String, ParameterValue]): Unit = {
      logger.debug(s"""Executing "$originalQueryText" with parameters $parameters.""")
    }

    object io {

      private def prepare(parameters: Map[String, ParameterValue])(implicit session: Session): core.BoundStatement = {
        val prepared = session.prepare(query.queryText)

        bind(prepared, parameters)
      }

      def execute(additionalParameters: Parameters = Parameters.empty)(implicit session: Session): core.ResultSet = {
        val parameters = setParameters(additionalParameters)
        logExecution(parameters)

        val prepared = prepare(parameters)
        session.execute(prepared)
      }

      def iterator(parameterValues: Parameters = Parameters.empty)(implicit session: Session): Iterator[T] = {
        val results = execute(parameterValues)
        results.iterator().asScala.map(convertRow)
      }

      def option(parameterValues: Parameters = Parameters.empty)(implicit session: Session): Option[T] = {
        val results = execute(parameterValues)
        Option(results.one()).map(convertRow)
      }

    }

    object future {

      private def prepare(parameters: Map[String, ParameterValue])(implicit session: Session, ec: ExecutionContext): Future[core.BoundStatement] = {
        for {
          prepared <- toScalaFuture(session.prepareAsync(query.queryText))
        } yield {
          bind(prepared, parameters)
        }
      }

      def execute(additionalParameters: Parameters = Parameters.empty)(implicit session: Session, ec: ExecutionContext): Future[core.ResultSet] = {
        val parameters = setParameters(additionalParameters)
        logExecution(parameters)

        for {
          prepared <- prepare(parameters)
          result <- implementation.toScalaFuture(session.executeAsync(prepared))
        } yield result
      }

      def iterator(parameterValues: Parameters = Parameters.empty)(implicit session: Session, ec: ExecutionContext): Future[Iterator[T]] = {
        for {
          result <- execute(parameterValues)
        } yield {
          result.iterator().asScala.map(convertRow)
        }
      }

      def option(parameterValues: Parameters = Parameters.empty)(implicit session: Session, ec: ExecutionContext): Future[Option[T]] = {
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

      def execute(additionalParameters: Parameters = Parameters.empty)(implicit session: Session): Task[core.ResultSet] = {
        val parameters = setParameters(additionalParameters)
        logExecution(parameters)

        for {
          prepared <- prepare(parameters)
          result <- toTask(session.executeAsync(prepared))
        } yield result
      }

      def iterator(additionalParameters: Parameters = Parameters.empty)(implicit session: Session): Task[Iterator[T]] = {
        for {
          result <- execute(additionalParameters)
        } yield {
          result.iterator().asScala.map(convertRow)
        }
      }

      def option(additionalParameters: Parameters = Parameters.empty)(implicit session: Session): Task[Option[T]] = {
        for {
          result <- execute(additionalParameters)
        } yield {
          Option(result.one()).map(convertRow)
        }
      }

    }

    def stream(additionalParameters: Parameters = Parameters.empty)(implicit session: Session): Process[Task, T] = {
      val parameters = setParameters(additionalParameters)
      logExecution(parameters)

      Process.await(task.prepare(parameters)) { bound =>
        val iterator = for {
          result <- toTask(session.executeAsync(bound))
        } yield {
          result.iterator().asScala.map(convertRow)
        }

        scalaz.stream.io.iterator(iterator)
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
        CompiledStatement(queryText, hasParameters),
        queryOptions,
        Map.empty[String, ParameterValue]
      )
    }

    object stream {
      def ofQueries[T](cluster: core.Cluster): Channel[Task, Query[T], Process[Task, T]] = {
        channel.lift[Task, Query[T], Process[Task, T]] { query =>
          Task.delay {
            Process.await(toTask(cluster.connectAsync())) {implicit session =>
              query.stream().onComplete(Process.eval_(toTask(session.closeAsync())))
            }
          }
        }
      }

      def ofQueriesWithKeyspace[T](cluster: core.Cluster): Channel[Task, (String, Query[T]), Process[Task, T]] = {
        channel.lift[Task, (String, Query[T]), Process[Task, T]] {
          case (keyspace, query) =>
            Task.delay {
              Process.await(toTask(cluster.connectAsync(keyspace))) {implicit session =>
                query.stream().onComplete(Process.eval_(toTask(session.closeAsync())))
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
        channel.lift[Task, Parameters, Process[Task, T]] { parameters =>
          Task.delay {
            Process.await(toTask(cluster.connectAsync())) {implicit session =>
              query.stream(parameters).onComplete(Process.eval_(toTask(session.closeAsync())))
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
        channel.lift[Task, (String, Parameters), Process[Task, T]] {
          case (keyspace, parameters) =>
            Task.delay {
              Process.await(toTask(cluster.connectAsync(keyspace))) {implicit session =>
                query.stream(parameters).onComplete(Process.eval_(toTask(session.closeAsync())))
              }
            }
        }
      }
    }
  }

}
