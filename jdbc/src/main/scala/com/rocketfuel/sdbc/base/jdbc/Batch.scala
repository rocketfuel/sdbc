package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logger
import fs2.{Pipe, Stream}
import fs2.util.Async
import scala.collection.parallel.immutable.ParMap

trait Batch {
  self: DBMS with Connection =>

  /*
  Override this if the DBMS supports executeLargeBatch.
  So far, none do.
   */
  protected def executeBatch(statement: PreparedStatement): IndexedSeq[Long] = {
    statement.executeBatch().map(_.toLong)
  }

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
    */
  case class Batch(
    queries: Map[CompiledStatement, Vector[Parameters]]
  ) {

    private def alter(query: CompiledParameterizedQuery[_], op: Vector[Parameters] => Vector[Parameters]): Batch = {
      if (! query.isComplete)
        throw new IllegalArgumentException("query must have all its parameters set before adding it to a batch")

      copy(queries = queries + (query.statement -> op(queries.getOrElse(query.statement, Vector.empty))))
    }

    def prepend(query: CompiledParameterizedQuery[_]): Batch = {
      alter(query, params => query.parameters +: params)
    }

    def append(query: CompiledParameterizedQuery[_]): Batch = {
      alter(query, params =>  params :+ query.parameters)
    }

    val +: : CompiledParameterizedQuery[_] => Batch = prepend

    val :+ : CompiledParameterizedQuery[_] => Batch = append

    def ++(other: Batch) =
      Batch(queries ++ other.queries)

    def batch()(implicit connection: Connection): Map[CompiledStatement, IndexedSeq[Long]] =
      Batch.batch(queries)

    def stream[F[_]]()(implicit pool: Pool, async: Async[F]): Stream[F, (CompiledStatement, IndexedSeq[Long])] =
      Batch.stream[F](queries.seq)

  }

  object Batch extends Logger {
    override protected def logClass = classOf[com.rocketfuel.sdbc.base.jdbc.Batch]

    def apply(queries: Seq[CompiledParameterizedQuery[_]]): Batch = {
      Batch(toBatches(queries))
    }

    def toBatches(queries: Seq[CompiledParameterizedQuery[_]]): Map[CompiledStatement, Vector[Parameters]] = {
      for {
        (statement, queries) <- queries.groupBy(_.statement)
      } yield (statement, queries.map(_.parameters).toVector)
    }

    /**
      * A minimal query to assist in creating batches.
      */
    case class Part(
      override val statement: CompiledStatement,
      override val parameters: Parameters = Parameters.empty
    ) extends CompiledParameterizedQuery[Part] {
      override protected def subclassConstructor(parameters: Parameters): Part =
        Part(statement, parameters)
    }

    protected def prepare(
      compiledStatement: CompiledStatement,
      batches: Vector[Parameters]
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
        setParameters(batch)
        prepared.addBatch()
      }
      prepared
    }

    def batch(
      queries: Map[CompiledStatement, Vector[Parameters]]
    )(implicit connection: Connection
    ): Map[CompiledStatement, IndexedSeq[Long]] = {
      for {
        (statement, params) <- queries
      } yield {
        statement -> {
          logRun(statement, params)
          val prepared = prepare(statement, params)
          val result = executeBatch(prepared)
          prepared.close()
          result
        }
      }
    }

    def stream[F[_]](
      queries: Map[CompiledStatement, Vector[Parameters]]
    )(implicit async: Async[F],
      pool: Pool
    ): Stream[F, (CompiledStatement, IndexedSeq[Long])] = {
      for {
        statementParams <- Stream[F, (CompiledStatement, Vector[Parameters])](queries.toSeq: _*)
        result <-
          Stream.bracket[F, Connection, (CompiledStatement, IndexedSeq[Long])](
            r = async.delay(pool.getConnection())
          )(use = {implicit connection: Connection =>
            Stream.bracket[F, PreparedStatement, (CompiledStatement, IndexedSeq[Long])](
              r = async.delay(prepare(statementParams._1, statementParams._2))
            )(use = statement => Stream(statementParams._1 -> executeBatch(statement)),
              release = statement => async.delay(statement.close())
            )
          },
            release = (connection: Connection) => async.delay(connection.close())
          )
      } yield result
    }

    /**
      * Run queries by taking chunks from an input stream and batching them together.
      * @param n is the maximum number of parameter sets to include in a chunk.
      * @param allowFewer means the batch must have exactly n parameter sets.
      */
    def pipe[F[_]](
      n: Int,
      allowFewer: Boolean = true
    )(implicit async: Async[F], pool: Pool
    ): Pipe[F, CompiledParameterizedQuery[_], Stream[F, (CompiledStatement, IndexedSeq[Long])]] =
      (queriesStream: Stream[F, CompiledParameterizedQuery[_]]) =>
        for {
          queries <- queriesStream.vectorChunkN(n, allowFewer)
        } yield stream(toBatches(queries))

    private def logRun(
      compiledStatement: CompiledStatement,
      batches: Vector[Parameters]
    ): Unit = {
      log.debug(s"""query "${compiledStatement.originalQueryText}"""")

      if (batches.isEmpty)
        log.warn("Executing a batch query without any batches.")
    }

  }

}
