package com.rocketfuel.sdbc.base.jdbc

import java.sql.SQLFeatureNotSupportedException
import com.rocketfuel.sdbc.base.Logger
import fs2._
import fs2.util.Async
import shapeless.ops.record.{MapValues, ToMap}
import shapeless.{HList, LabelledGeneric}

trait Batch {
  self: DBMS with Connection =>

  /**
    * Create and run a batch using a statement and a sequence of parameters.
    *
    * Batch contains two collections of parameters. One is a list of parameters for building a batch,
    * and a list of batches. Batches can be built using {@link #on} and finalized with {@link #addBatch},
    * or by passing parameters to {@link #addBatch}.
    *
    * @param statement
    * @param defaultParameters are parameters included in each batch.
    * @param batches are parameters to add or replace the defaultParameters for each batch.
    */
  case class Batch(
    statement: CompiledStatement,
    defaultParameters: Parameters = Parameters.empty,
    batches: ParameterBatches = Vector.empty[Parameters]
  ) extends ParameterizedQuery[Batch] {
    batchSelf =>

    override def parameters: Parameters = defaultParameters

    def addParameters(batchParameters: Parameters): Batch = {
      copy(batches = batches :+ batchParameters)
    }

    def add(additionalParameter: (String, ParameterValue), additionalParameters: (String, ParameterValue)*): Batch = {
      addParameters(Map((additionalParameter +: additionalParameters): _*))
    }

    def addProduct[
      A,
      Repr <: HList,
      Key <: Symbol,
      AsParameters <: HList
    ](t: A
    )(implicit p: Parameters.Products[A, Repr, Key, AsParameters]
    ): Batch = {
      addParameters(Parameters.product(t))
    }

    def addRecord[
      Repr <: HList,
      Key <: Symbol,
      AsParameters <: HList
    ](t: Repr
    )(implicit r: Parameters.Records[Repr, Key, AsParameters]
    ): Batch = {
      addParameters(Parameters.record(t))
    }

    def batch()(implicit connection: Connection): IndexedSeq[Long] = {
      Batch.batch(statement, defaultParameters, batches)
    }

    def pipe[F[_]](implicit async: Async[F]): Batch.Pipe[F] =
      Batch.Pipe[F](statement, parameters)

    def sink[F[_]](implicit async: Async[F]): Batch.Sink[F] =
      Batch.Sink[F](statement, parameters)

    override protected def subclassConstructor(
      parameters: Parameters
    ): Batch = {
      copy(defaultParameters = parameters)
    }

  }

  object Batch
    extends Logger {

    protected def prepare(
      compiledStatement: CompiledStatement,
      defaultParameters: Parameters,
      batches: ParameterBatches
    )(implicit connection: Connection
    ): PreparedStatement = {
      val prepared = connection.prepareStatement(compiledStatement.queryText)

      def setParameters(parameters: Parameters): Unit = {
        for (nameValue <- parameters) {
          val (name, value) = nameValue
          for (indexes <- compiledStatement.parameterPositions.get(name)) {
            for (index <- indexes)
              value.set(prepared, index)
          }
        }
      }

      for (batch <- batches) {
        setParameters(defaultParameters)
        setParameters(batch)
        prepared.addBatch()
      }
      prepared
    }

    def batch(
      compiledStatement: CompiledStatement,
      defaultParameters: Parameters = Parameters.empty,
      batches: ParameterBatches
    )(implicit connection: Connection
    ): IndexedSeq[Long] = {

      val prepared = prepare(compiledStatement, defaultParameters, batches)

      logRun(compiledStatement, batches)

      val result = try {
        prepared.executeLargeBatch()
      } catch {
        case _: UnsupportedOperationException |
             _: SQLFeatureNotSupportedException =>
          prepared.executeBatch().map(_.toLong)
      }
      prepared.close()
      result.toVector
    }

    case class Sink[F[_]](
      statement: CompiledStatement,
      defaultParameters: Parameters = Parameters.empty
    )(implicit async: Async[F]
    ) {
      def parameters(implicit pool: Pool): fs2.Pipe[F, Seq[Parameters], Unit] = {
        paramStream =>
          for {
            params <- paramStream
            result <- Stream.bracket[F, Connection, IndexedSeq[Long]](
              r = async.delay(pool.getConnection())
            )(use = {implicit connection: Connection => Stream.eval(async.delay(Batch.batch(statement, defaultParameters, params)))},
              release = connection => async.delay(connection.close())
            )
          } yield ()
      }

      def products[
        A,
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        p: Parameters.Products[A, Repr, Key, AsParameters]
      ): fs2.Pipe[F, Seq[A], Unit] = {
        _.map(_.map(Parameters.product[A, Repr, Key, AsParameters])).to(parameters)
      }

      def records[
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        r: Parameters.Records[Repr, Key, AsParameters]
      ): fs2.Pipe[F, Seq[Repr], Unit] = {
        _.map(_.map(Parameters.record[Repr, Key, AsParameters])).to(parameters)
      }
    }

    case class Pipe[F[_]](
      statement: CompiledStatement,
      defaultParameters: Parameters = Parameters.empty
    )(implicit async: Async[F]
    ) {
      def parameters(implicit pool: Pool): fs2.Pipe[F, Seq[Parameters], IndexedSeq[Long]] = {
        pipe.lift[F, Seq[Parameters], IndexedSeq[Long]] { params =>
          val withParams =
            params.map(defaultParameters ++ _)

          pool.withConnection { implicit connection =>
            Batch.batch(statement, defaultParameters, withParams)
          }
        }
      }

      def products[
        A,
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        p: Parameters.Products[A, Repr, Key, AsParameters]
      ): fs2.Pipe[F, Seq[A], IndexedSeq[Long]] = {
        _.map(_.map(Parameters.product[A, Repr, Key, AsParameters])).through(parameters)
      }

      def records[
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit pool: Pool,
        r: Parameters.Records[Repr, Key, AsParameters]
      ): fs2.Pipe[F, Seq[Repr], IndexedSeq[Long]] = {
        _.map(_.map(Parameters.record[Repr, Key, AsParameters])).through(parameters)
      }
    }

    private def logRun(
      compiledStatement: CompiledStatement,
      batches: Seq[Parameters]
    ): Unit = {
      log.debug(s"""Executing batch of "${compiledStatement.originalQueryText}".""")

      if (batches.isEmpty) log.warn("Executing a batch query without any batches.")
    }

  }

}
