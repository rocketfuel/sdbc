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
    * Batch contains two collections of parameters. One is the current batch,
    * and the other is a list of batches. Methods from [[ParameterizedQuery]]
    * act on the current batch. The current batch is added to the list of batches
    * using {@link #add}, {@link #addParameters}, {@link #addProduct}, or
    * {@link #addRecord}. The `add` methods perform a union with the current parameter
    * list and append the result to the list of batches. The current parameter list
    * is not altered.
    *
    * If you don't want to use the concept of default parameters for the batches,
    * simply disregard the `on` methods from [[ParameterizedQuery]], and use only the
    * `add` methods.
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
    q =>

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

    /**
      * Get helper methods for creating [[Batchable]]s from this query.
      */
    def batchable[Key]: ToBatchable[Key] =
      new ToBatchable[Key]

    class ToBatchable[Key] {
      def constant: Batchable[Key] =
        Batchable(Function.const(q))

      def parameters(toParameters: Key => TraversableOnce[Parameters]): Batchable[Key] =
        new Batchable[Key] {
          override def batch(key: Key): Batch = {
            val batches = toParameters(key)
            batches.foldLeft(q) {
              case (batchAccum, params) =>
                batchAccum.addParameters(params)
            }
          }
        }

      def product[
        P,
        Repr <: HList,
        HMapKey <: Symbol,
        AsParameters <: HList
      ](toProducts: Key => Traversable[P]
      )(implicit p: Parameters.Products[P, Repr, HMapKey, AsParameters]
      ): Batchable[Key] =
        parameters((key: Key) => toProducts(key).map(Parameters.product(_)))

      def record[
        Repr <: HList,
        HMapKey <: Symbol,
        AsParameters <: HList
      ](toRecords: Key => Traversable[Repr]
      )(implicit p: Parameters.Records[Repr, HMapKey, AsParameters],
        ev: Repr =:= Key
      ): Batchable[Key] =
        parameters((key: Key) => toRecords(key).map(Parameters.record(_)))
    }

  }

  object Batch
    extends Logger {

    override protected def logClass: Class[_] = classOf[com.rocketfuel.sdbc.base.jdbc.Batch]

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
      log.debug(s"""query "${compiledStatement.originalQueryText}"""")

      if (batches.isEmpty)
        log.warn("Executing a batch query without any batches.")
    }

  }

}
