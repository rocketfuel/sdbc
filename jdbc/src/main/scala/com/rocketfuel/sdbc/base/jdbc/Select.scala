package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.{CloseableIterator, Logger}
import fs2.{Stream, pipe}
import fs2.util.Async
import java.io.InputStream
import java.net.URL
import java.nio.file.Path
import scala.reflect.ClassTag
import shapeless.HList

trait Select {
  self: DBMS with Connection =>

  /**
    * Represents a query that is ready to be run against a [[Connection]].
    * @param statement is the text of the query. You can supply a String, and it will be converted to a
    *                  [[CompiledStatement]] by [[com.rocketfuel.sdbc.base.CompiledStatement.apply(String)]].
    * @param parameters
    * @param rowConverter
    * @tparam A
    */
  case class Select[A](
    override val statement: CompiledStatement,
    override val parameters: Parameters = Parameters.empty
  )(implicit rowConverter: RowConverter[A]
  ) extends IgnorableQuery[Select[A]] {
    q =>

    def map[B](f: A => B): Select[B] = {
      implicit val innerConverter: Row => B = rowConverter.andThen(f)
      Select[B](statement, parameters)
    }

    override def subclassConstructor(parameters: Parameters): Select[A] = {
      copy(parameters = parameters)
    }

    def as[B](implicit otherRowConverter: RowConverter[B]): Select[B] =
      Select[B](statement, parameters)

    def iterator()(implicit connection: Connection): CloseableIterator[A] = {
      Select.iterator(statement, parameters)
    }

    def vector()(implicit connection: Connection): Vector[A] = {
      Select.vector(statement, parameters)
    }

    def option()(implicit connection: Connection): Option[A] = {
      Select.option(statement, parameters)
    }

    def one()(implicit connection: Connection): A = {
      Select.one(statement, parameters)
    }

    def stream[F[_]](implicit
      async: Async[F],
      pool: Pool
    ): Stream[F, A] = {
      Select.stream[F, A](statement, parameters)
    }

    def pipe[F[_]](implicit async: Async[F]): Select.Pipe[F, A] =
      Select.Pipe(statement, parameters)

    /**
      * Get helper methods for creating [[Selectable]]s from this query.
      */
    def selectable[Key]: ToSelectable[Key] =
      new ToSelectable[Key]

    class ToSelectable[Key] {
      def constant: Selectable[Key, A] =
        Function.const(q) _

      def parameters(toParameters: Key => Parameters): Selectable[Key, A] =
        key => q.onParameters(toParameters(key))

      def product[
        Repr <: HList,
        HMapKey <: Symbol,
        AsParameters <: HList
      ](implicit p: Parameters.Products[Key, Repr, HMapKey, AsParameters]
      ): Selectable[Key, A] =
        parameters(Parameters.product(_))

      def record[
        Repr <: HList,
        HMapKey <: Symbol,
        AsParameters <: HList
      ](implicit p: Parameters.Records[Repr, HMapKey, AsParameters],
        ev: Repr =:= Key
      ): Selectable[Key, A] =
        parameters(key => Parameters.record(key.asInstanceOf[Repr]))
    }

  }

  object Select
    extends Logger {

    override protected def logClass: Class[_] = classOf[com.rocketfuel.sdbc.base.jdbc.Select]

    def readInputStream[
      A
    ](stream: InputStream
    )(implicit rowConverter: RowConverter[A],
      codec: scala.io.Codec = scala.io.Codec.default
    ): Select[A] = {
      Select[A](CompiledStatement.readInputStream(stream))
    }

    def readUrl[
      A
    ](u: URL
    )(implicit rowConverter: RowConverter[A],
      codec: scala.io.Codec = scala.io.Codec.default
    ): Select[A] = {
      Select[A](CompiledStatement.readUrl(u))
    }

    def readPath[
      A
    ](path: Path
    )(implicit rowConverter: RowConverter[A],
      codec: scala.io.Codec = scala.io.Codec.default
    ): Select[A] = {
      Select[A](CompiledStatement.readPath(path))
    }

    def readClassResource[
      A
    ](clazz: Class[_],
      name: String,
      nameMangler: (Class[_], String) => String = CompiledStatement.NameManglers.default
    )(implicit rowConverter: RowConverter[A],
      codec: scala.io.Codec = scala.io.Codec.default
    ): Select[A] = {
      Select[A](CompiledStatement.readClassResource(clazz, name, nameMangler))
    }

    def readTypeResource[
      ResourceType,
      Row
    ](name: String,
      nameMangler: (Class[_], String) => String = CompiledStatement.NameManglers.default
    )(implicit rowConverter: RowConverter[Row],
      codec: scala.io.Codec = scala.io.Codec.default,
      tag: ClassTag[ResourceType]
    ): Select[Row] = {
      Select[Row](CompiledStatement.readTypeResource[ResourceType](name, nameMangler))
    }


    def readResource[
      A
    ](name: String
    )(implicit rowConverter: RowConverter[A],
      codec: scala.io.Codec = scala.io.Codec.default
    ): Select[A] = {
      Select[A](CompiledStatement.readResource(name))
    }

    def iterator[A](
      statement: CompiledStatement,
      parameterValues: Parameters = Parameters.empty
    )(implicit connection: Connection,
      rowConverter: RowConverter[A]
    ): CloseableIterator[A] = {
      QueryCompanion.logRun(log, statement, parameterValues)
      val executed = QueryMethods.execute(statement, parameterValues)
      StatementConverter.convertedRowIterator[A](executed)
    }

    def option[A](
      statement: CompiledStatement,
      parameterValues: Parameters = Parameters.empty
    )(implicit connection: Connection,
      rowConverter: RowConverter[A]
    ): Option[A] = {
      QueryCompanion.logRun(log, statement, parameterValues)
      val executed = QueryMethods.execute(statement, parameterValues)
      try StatementConverter.convertedRowOption(executed)
      finally executed.close()
    }

    def one[A](
      statement: CompiledStatement,
      parameterValues: Parameters = Parameters.empty
    )(implicit connection: Connection,
      rowConverter: RowConverter[A]
    ): A = {
      QueryCompanion.logRun(log, statement, parameterValues)
      val executed = QueryMethods.execute(statement, parameterValues)
      try StatementConverter.convertedRowOne(executed)
      finally executed.close()
    }

    def vector[A](
      statement: CompiledStatement,
      parameterValues: Parameters = Parameters.empty
    )(implicit connection: Connection,
      rowConverter: RowConverter[A]
    ): Vector[A] = {
      QueryCompanion.logRun(log, statement, parameterValues)
      val executed = QueryMethods.execute(statement, parameterValues)

      try StatementConverter.convertedRowVector[A](executed)
      finally executed.close()
    }

    def stream[F[_], A](
      statement: CompiledStatement,
      parameterValues: Parameters = Parameters.empty
    )(implicit async: Async[F],
      pool: Pool,
      rowConverter: RowConverter[A]
    ): Stream[F, A] = {
      Stream.bracket[F, Connection, A](
        r = async.delay(pool.getConnection())
      )(use = {implicit connection: Connection =>
          CloseableIterator.toStream(async.delay(iterator[A](statement, parameterValues)))
      },
        release = (connection: Connection) => async.delay(connection.close())
      )
    }

    case class Pipe[F[_], A](
      statement: CompiledStatement,
      defaultParameters: Parameters = Parameters.empty
    )(implicit async: Async[F],
      rowConverter: RowConverter[A]
    ) {
      private val parameterPipe = Parameters.Pipe[F]

      def parameters(implicit pool: Pool): fs2.Pipe[F, Parameters, Stream[F, A]] = {
        parameterPipe.combine(defaultParameters).andThen(
          pipe.lift[F, Parameters, Stream[F, A]] { params =>
            stream(statement, params)
          }
        )
      }

      def products[
        B,
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        p: Parameters.Products[B, Repr, Key, AsParameters]
      ): fs2.Pipe[F, B, Stream[F, A]] = {
        parameterPipe.products.andThen(parameters)
      }

      def records[
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        r: Parameters.Records[Repr, Key, AsParameters]
      ): fs2.Pipe[F, Repr, Stream[F, A]] = {
        parameterPipe.records.andThen(parameters)
      }

    }

    implicit val partable: Batch.Partable[Select[_]] =
      (q: Select[_]) => Batch.Part(q.statement, q.parameters)

  }

}
