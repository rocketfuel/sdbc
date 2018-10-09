package com.rocketfuel.sdbc.cassandra

import com.rocketfuel.sdbc.base.{CompiledStatement, Logger}
import com.rocketfuel.sdbc.Cassandra._
import fs2._
import fs2.util.Async
import fs2.util.syntax._
import java.io.InputStream
import java.net.URL
import java.nio.file.Path
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import shapeless.HList

/**
  * Represents a query that is ready to be run against a [[Session]].
  * @param statement is the text of the query. You can supply a String, and it will be converted to a
  *                  [[CompiledStatement]] by [[CompiledStatement!.apply(String)]].
  * @param parameters
  * @param queryOptions
  * @param converter
  * @tparam A
  */
case class Query[A](
  override val statement: CompiledStatement,
  override val parameters: Parameters = Parameters.empty,
  queryOptions: QueryOptions = QueryOptions.default
)(implicit val converter: RowConverter[A]
) extends CompiledParameterizedQuery[Query[A]] {
  q =>

  def map[B](f: A => B): Query[B] = {
    implicit val innerConverter: Row => B = converter.andThen(f)
    Query[B](statement, parameters)
  }

  override protected def subclassConstructor(parameters: Parameters): Query[A] = {
    copy(parameters = parameters)
  }

  /**
    * Get helper methods for creating [[Queryable]]s from this query.
    */
  def queryable[Key]: ToQueryable[Key] =
    new ToQueryable[Key]

  class ToQueryable[Key] {
    def constant: Queryable[Key, A] =
      Queryable(Function.const(q))

    def parameters(toParameters: Key => Parameters): Queryable[Key, A] =
      Queryable(key => q.onParameters(toParameters(key)))

    def product[
      Repr <: HList,
      HMapKey <: Symbol,
      AsParameters <: HList
    ](implicit p: Parameters.Products[Key, Repr, HMapKey, AsParameters]
    ): Queryable[Key, A] =
      parameters(Parameters.product(_))

    def record[
      Repr <: HList,
      HMapKey <: Symbol,
      AsParameters <: HList
    ](implicit p: Parameters.Records[Repr, HMapKey, AsParameters],
      ev: Repr =:= Key
    ): Queryable[Key, A] =
      parameters(key => Parameters.record(key.asInstanceOf[Repr]))
  }

  def execute()(implicit session: Session): ResultSet = {
    Query.execute(statement, parameters, queryOptions)
  }

  def iterator()(implicit session: Session): Iterator[A] = {
    Query.iterator(statement, parameters, queryOptions)
  }

  def option()(implicit session: Session): Option[A] = {
    Query.option(statement, parameters, queryOptions)
  }

  def one()(implicit session: Session): A = {
    Query.one(statement, parameters, queryOptions)
  }

  def stream[F[_]](implicit session: Session, async: Async[F]): Stream[F, A] = {
    Query.stream[F, A](statement, parameters, queryOptions)
  }

  object future {

    def execute()(implicit session: Session, ec: ExecutionContext): Future[ResultSet] = {
      Query.future.execute(statement, queryOptions, parameters)
    }

    def iterator()(implicit session: Session, ec: ExecutionContext): Future[Iterator[A]] = {
      Query.future.iterator(statement, queryOptions, parameters)
    }

    def option()(implicit session: Session, ec: ExecutionContext): Future[Option[A]] = {
      Query.future.option(statement, queryOptions, parameters)
    }

    def one()(implicit session: Session, ec: ExecutionContext): Future[A] = {
      Query.future.one(statement, queryOptions, parameters)
    }

  }

  object async {

    def execute[F[_]]()(implicit session: Session, async: Async[F]): F[ResultSet] = {
      Query.async.execute(statement, queryOptions, parameters)
    }

    def iterator[F[_]]()(implicit session: Session, async: Async[F]): F[Iterator[A]] = {
      Query.async.iterator(statement, queryOptions, parameters)
    }

    def option[F[_]]()(implicit session: Session, async: Async[F]): F[Option[A]] = {
      Query.async.option(statement, queryOptions, parameters)
    }

    def one[F[_]]()(implicit session: Session, async: Async[F]): F[A] = {
      Query.async.one(statement, queryOptions, parameters)
    }

  }

  object task {

    def execute()(implicit session: Session, strategy: Strategy): Task[ResultSet] = {
      async.execute[Task]()
    }

    def iterator(implicit session: Session, strategy: Strategy): Task[Iterator[A]] = {
      async.iterator[Task]()
    }

    def option()(implicit session: Session, strategy: Strategy): Task[Option[A]] = {
      async.option[Task]()
    }

    def one()(implicit session: Session, strategy: Strategy): Task[A] = {
      async.one[Task]()
    }

  }

  def pipe[F[_]](implicit async: Async[F]): Query.Pipe[F, A] =
    new Query.Pipe[F, A](statement, parameters, queryOptions)

  def sink[F[_]](implicit async: Async[F]): Query.Sink[F] =
    new Query.Sink[F](statement, parameters, queryOptions)

}

object Query
  extends QueryCompanionOps
    with Logger {

  override protected def logClass: Class[_] = classOf[Query[_]]

  def readInputStream[A](
    stream: InputStream,
    queryOptions: QueryOptions = QueryOptions.default
  )(implicit rowConverter: RowConverter[A],
    codec: scala.io.Codec = scala.io.Codec.default
  ): Query[A] = {
    Query[A](CompiledStatement.readInputStream(stream))
  }

  def readUrl[
    A
  ](u: URL,
    queryOptions: QueryOptions = QueryOptions.default
  )(implicit rowConverter: RowConverter[A],
    codec: scala.io.Codec = scala.io.Codec.default
  ): Query[A] = {
    Query[A](CompiledStatement.readUrl(u), queryOptions = queryOptions)
  }

  def readPath[
    A
  ](path: Path,
    queryOptions: QueryOptions = QueryOptions.default
  )(implicit rowConverter: RowConverter[A],
    codec: scala.io.Codec = scala.io.Codec.default
  ): Query[A] = {
    Query[A](CompiledStatement.readPath(path), queryOptions = queryOptions)
  }

  def readClassResource[
    A
  ](clazz: Class[_],
    name: String,
    queryOptions: QueryOptions = QueryOptions.default,
    nameMangler: (Class[_], String) => String = CompiledStatement.NameManglers.default
  )(implicit rowConverter: RowConverter[A],
    codec: scala.io.Codec = scala.io.Codec.default
  ): Query[A] = {
    Query[A](CompiledStatement.readClassResource(clazz, name, nameMangler), queryOptions = queryOptions)
  }

  def readTypeResource[
    ResourceType,
    Row
  ](name: String,
    queryOptions: QueryOptions = QueryOptions.default,
    nameMangler: (Class[_], String) => String = CompiledStatement.NameManglers.default
  )(implicit rowConverter: RowConverter[Row],
    codec: scala.io.Codec = scala.io.Codec.default,
    tag: ClassTag[ResourceType]
  ): Query[Row] = {
    Query[Row](CompiledStatement.readTypeResource[ResourceType](name, nameMangler), queryOptions = queryOptions)
  }

  def readResource[
    A
  ](name: String,
    queryOptions: QueryOptions = QueryOptions.default
  )(implicit rowConverter: RowConverter[A],
    codec: scala.io.Codec = scala.io.Codec.default
  ): Query[A] = {
    Query[A](CompiledStatement.readResource(name), queryOptions = queryOptions)
  }

  def execute(
    statement: CompiledStatement,
    parameters: Parameters = Parameters.empty,
    queryOptions: QueryOptions = QueryOptions.default
  )(implicit session: Session
  ): ResultSet = {
    logExecution(statement, parameters)

    val prepared = session.prepare(statement.queryText)
    val bound = bind(statement, prepared, parameters, queryOptions)
    session.execute(bound)
  }

  def iterator[A](
    statement: CompiledStatement,
    parameters: Parameters = Parameters.empty,
    queryOptions: QueryOptions = QueryOptions.default
  )(implicit converter: RowConverter[A],
    session: Session
  ): Iterator[A] = {
    execute(statement, parameters, queryOptions).iterator().asScala.map(converter)
  }

  def stream[F[_], A](
    statement: CompiledStatement,
    parameters: Parameters = Parameters.empty,
    queryOptions: QueryOptions = QueryOptions.default
  )(implicit converter: RowConverter[A],
    session: Session,
    async: Async[F]
  ): Stream[F, A] = {

    def currentChunk(results: ResultSet, chunkSize: Int): Stream[F, A] = {
      val chunk = Vector.fill[A](chunkSize)(results.one())
      Stream.chunk(Chunk.seq(chunk))
    }

    def chunks(results: ResultSet): Stream[F, A] = {
      (results.getAvailableWithoutFetching, results.isFullyFetched) match {
        case (0, false) =>
          for {
            nextResults <- Stream.eval(toAsync[F, ResultSet](results.fetchMoreResults()))
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
      results <- Stream.eval(this.async.execute(statement, queryOptions, parameters))
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

  def one[A](
    statement: CompiledStatement,
    parameters: Parameters = Parameters.empty,
    queryOptions: QueryOptions = QueryOptions.default
  )(implicit converter: RowConverter[A],
    session: Session
  ): A = {
    Option(execute(statement, parameters, queryOptions).one()).map(converter).
      getOrElse(throw new NoSuchElementException("empty ResultSet"))
  }

  sealed abstract class PipeAux[F[_], A] private[Query] (
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
      p: Parameters.Products[B, Repr, Key, AsParameters]
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
      r: Parameters.Records[Repr, Key, AsParameters]
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

/**
  * This class exists merely to solve the name clash that occurs
  * when a class and its companion object have objects with the same
  * name.
  */
trait QueryCompanionOps {
  self: Logger =>

  protected def bind(
    statement: CompiledStatement,
    prepared: com.datastax.driver.core.PreparedStatement,
    parameters: Parameters,
    queryOptions: QueryOptions
  )(implicit session: Session
  ): PreparedStatement = {
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
    log.debug(s"""query "${statement.originalQueryText}", parameters $parameters""")

  object async {

    private def prepare[F[_]](
      statement: CompiledStatement
    )(implicit session: Session,
      async: Async[F]
    ): F[com.datastax.driver.core.PreparedStatement] = {
      toAsync(session.prepareAsync(statement.queryText))
    }

    def execute[F[_]](
      statement: CompiledStatement,
      queryOptions: QueryOptions,
      parameters: Parameters
    )(implicit session: Session,
      async: Async[F]
    ): F[ResultSet] = {
      logExecution(statement, parameters)

      for {
        prepared <- prepare(statement)
        bound = bind(statement, prepared, parameters, queryOptions)
        executed <- toAsync(session.executeAsync(bound))
      } yield executed
    }

    def iterator[F[_], A](
      statement: CompiledStatement,
      queryOptions: QueryOptions,
      parameters: Parameters
    )(implicit converter: RowConverter[A],
      session: Session,
      async: Async[F]
    ): F[Iterator[A]] = {
      for (results <- execute(statement, queryOptions, parameters)) yield
        for (result <- results.iterator().asScala) yield
          result
    }

    def option[F[_], A](
      statement: CompiledStatement,
      queryOptions: QueryOptions,
      parameters: Parameters
    )(implicit converter: RowConverter[A],
      session: Session,
      async: Async[F]
    ): F[Option[A]] = {
      for (results <- execute(statement, queryOptions, parameters)) yield
        for (result <- Option(results.one())) yield
          result
    }

    def one[F[_], A](
      statement: CompiledStatement,
      queryOptions: QueryOptions,
      parameters: Parameters
    )(implicit converter: RowConverter[A],
      session: Session,
      async: Async[F]
    ): F[A] = {
      option(statement, queryOptions, parameters).map(_.get)
    }

  }

  object task {

    def execute(
      statement: CompiledStatement,
      queryOptions: QueryOptions,
      parameters: Parameters
    )(implicit session: Session,
      strategy: Strategy
    ): Task[ResultSet] = {
      async.execute[Task](statement, queryOptions, parameters)
    }

    def iterator[A](
      statement: CompiledStatement,
      queryOptions: QueryOptions,
      parameters: Parameters
    )(implicit converter: RowConverter[A],
      session: Session,
      strategy: Strategy
    ): Task[Iterator[A]] = {
      async.iterator[Task, A](statement, queryOptions, parameters)
    }

    def option[A](
      statement: CompiledStatement,
      queryOptions: QueryOptions,
      parameters: Parameters
    )(implicit converter: RowConverter[A],
      session: Session,
      strategy: Strategy
    ): Task[Option[A]] = {
      async.option[Task, A](statement, queryOptions, parameters)
    }

    def one[A](
      statement: CompiledStatement,
      queryOptions: QueryOptions,
      parameters: Parameters
    )(implicit converter: RowConverter[A],
      session: Session,
      strategy: Strategy
    ): Task[A] = {
      async.one[Task, A](statement, queryOptions, parameters)
    }
  }

  object future {

    private def prepare(
      statement: CompiledStatement
    )(implicit session: Session,
      ec: ExecutionContext
    ): Future[com.datastax.driver.core.PreparedStatement] = {
      toScalaFuture(session.prepareAsync(statement.queryText))
    }

    def execute(
      statement: CompiledStatement,
      queryOptions: QueryOptions,
      parameters: Parameters
    )(implicit session: Session,
      ec: ExecutionContext
    ): Future[ResultSet] = {
      logExecution(statement, parameters)

      for {
        prepared <- prepare(statement)
        bound = bind(statement, prepared, parameters, queryOptions)
        results <- toScalaFuture(session.executeAsync(bound))
      } yield results
    }

    def iterator[A](
      statement: CompiledStatement,
      queryOptions: QueryOptions,
      parameters: Parameters
    )(implicit converter: RowConverter[A],
      session: Session,
      ec: ExecutionContext
    ): Future[Iterator[A]] = {
      for (results <- execute(statement, queryOptions, parameters)) yield
        for (result <- results.iterator().asScala) yield
          result
    }

    def option[A](
      statement: CompiledStatement,
      queryOptions: QueryOptions,
      parameters: Parameters
    )(implicit converter: RowConverter[A],
      session: Session,
      ec: ExecutionContext
    ): Future[Option[A]] = {
      for (results <- execute(statement, queryOptions, parameters)) yield
        for (result <- Option(results.one())) yield
          result
    }

    def one[A](
      statement: CompiledStatement,
      queryOptions: QueryOptions,
      parameters: Parameters
    )(implicit converter: RowConverter[A],
      session: Session,
      ec: ExecutionContext
    ): Future[A] = {
      option(statement, queryOptions, parameters).map(_.get)
    }

  }
}
