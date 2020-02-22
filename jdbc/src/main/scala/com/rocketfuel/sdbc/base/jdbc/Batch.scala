package com.rocketfuel.sdbc.base.jdbc

import cats.Eq
import cats.effect.Async
import com.rocketfuel.sdbc.base.Logger
import fs2.{Chunk, Pipe, Stream}

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
  class Batch private (
    val batches: Map[CompiledStatement, Vector[Parameters]]
  ) {
    q =>

    def this() {
      this(Map.empty)
    }

    private def alter(
      query: CompiledParameterizedQuery[_],
      op: Vector[Parameters] => Vector[Parameters]
    ): Batch = {
      if (! query.isComplete)
        throw new IllegalArgumentException("query must have all its parameters set before adding it to a batch")

      new Batch(batches + (query.statement -> op(batches.getOrElse(query.statement, Vector.empty))))
    }

    def prepend(query: CompiledParameterizedQuery[_]): Batch = {
      alter(query, params => query.parameters +: params)
    }

    def append(query: CompiledParameterizedQuery[_]): Batch = {
      alter(query, params =>  params :+ query.parameters)
    }

    val +: : CompiledParameterizedQuery[_] => Batch = prepend

    val :+ : CompiledParameterizedQuery[_] => Batch = append

    def ++(other: Batch): Batch = {
      new Batch(
        other.batches.foldLeft(batches) {
          case (result, (statement, otherParams)) =>
            result + (statement -> (result.getOrElse(statement, Vector.empty) ++ otherParams))
        }
      )
    }

    def batch()(implicit connection: Connection): Batch.Results =
      Batch.batch(batches)

    def stream[F[_]]()(implicit pool: Pool, async: Async[F]): Stream[F, Batch.Result] =
      Batch.stream[F](batches)

  }

  object Batch extends Logger {

    def apply(queries: CompiledParameterizedQuery[_]*): Batch = {
      queries.foreach(check)
      new Batch(toBatches(queries))
    }

    def apply(queries: Vector[CompiledParameterizedQuery[_]]): Batch = {
      apply(queries: _*)
    }

    def unapply(b: Batch): Option[Map[CompiledStatement, Vector[Parameters]]] =
      Some(b.batches)

    private def check(query: CompiledParameterizedQuery[_]): Unit = {
      if (!query.isComplete) {
        throw new IllegalArgumentException("query must have all its parameters set before adding it to a batch")
      }
    }

    val empty = new Batch(Map.empty)

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

    trait Partable[Key] extends (Key => Part)

    object Partable {
      def apply[Key](implicit p: Partable[Key]): Partable[Key] = p

      implicit def create[Key](f: Key => Part): Partable[Key] =
        new Partable[Key] {
          override def apply(key: Key): Part =
            f(key)
        }
    }

    trait syntax {
      implicit class QuerySeqMethods(queries: Seq[CompiledParameterizedQuery[_]]) {
        def batches()(implicit connection: Connection): Results = {
          Batch.batch(queries: _*)
        }
      }
    }

    object syntax extends syntax

    case class Result(
      statement: CompiledStatement,
      updateCounts: Vector[(Parameters, Long)]
    ) {
      lazy val sum: Long =
        updateCounts.map(_._2).sum

      def parts: Vector[Part] =
        for {
          (parameters, _) <- updateCounts
        } yield Part(statement, parameters)
    }

    case class Results(
      results: Map[CompiledStatement, Vector[(Parameters, Long)]]
    ) {
      lazy val sum: Long = {
        for {
          (_, _, count) <- unzip
        } yield count
      }.sum

      def sumByStatement: Map[CompiledStatement, Long] =
        for {
          (statement, statementResults) <- results
        } yield statement -> statementResults.map(_._2).sum

      def unzip: Iterable[(CompiledStatement, Parameters, Long)] =
        for {
          (statement, statementResults) <- results
          (params, count) <- statementResults
        } yield (statement, params, count)

      /**
        * Reconstruct the queries that created this batch result.
        */
      def queries: Iterable[Part] =
        for {
          (statement, params, _) <- unzip
        } yield Part(statement, params)

      /**
        * Reconstruct the batch that created this batch result.
        */
      def batch: Batch =
        Batch(queries.toVector)
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

    private def prepareAndRun(
      statement: CompiledStatement,
      batches: Vector[Parameters]
    )(implicit connection: Connection
    ): IndexedSeq[Long] = {
      val preparedStatement = prepare(statement, batches)
      try {
        logRun(statement, batches)
        executeBatch(preparedStatement)
      } finally preparedStatement.close()
    }

    def batch(
      queries: CompiledParameterizedQuery[_]*
    )(implicit connection: Connection
    ): Results = {
      batch(toBatches(queries))
    }

    def batch(
      batches: Map[CompiledStatement, Vector[Parameters]]
    )(implicit connection: Connection
    ): Results = {
      val results =
        for {
          (statement, batches) <- batches
        } yield statement -> batches.zip(prepareAndRun(statement, batches))
      Results(results)
    }

    def stream[F[_]](
      queries: CompiledParameterizedQuery[_]*
    )(implicit async: Async[F],
      pool: Pool
    ): Stream[F, Result] = {
      stream(toBatches(queries))
    }

    def stream[F[_]](
      batches: Map[CompiledStatement, Vector[Parameters]]
    )(implicit async: Async[F],
      pool: Pool
    ): Stream[F, Result] = {
      for {
        statementAndBatches <- Stream[F, (CompiledStatement, Vector[Parameters])](batches.toSeq: _*)
        (statement, batches) = statementAndBatches
        result <-
          Stream.eval(
            async.delay {
              pool.withConnection {implicit connection =>
                try {
                  Result(statement, batches.zip(prepareAndRun(statement, batches)))
                } finally connection.close()
              }
            }
          )
      } yield result
    }

    private implicit val compiledStatementEq: Eq[CompiledStatement] =
      new Eq[CompiledStatement] {
        override def eqv(
          x: CompiledStatement,
          y: CompiledStatement
        ): Boolean = {
          x.queryText == y.queryText
        }
      }

    /**
      * Run queries by taking chunks from an input stream and batching parameters with
      * common statements together.
      *
      * Use the various `chunk` methods on your input stream to group the appropriate
      * number of queries together for batching. See [[Stream.groupAdjacentBy]].
      */
    def pipe[
      F[_]
    ](implicit async: Async[F],
      pool: Pool
    ): Pipe[F, CompiledParameterizedQuery[_], Result] =
      (queriesStream: Stream[F, CompiledParameterizedQuery[_]]) =>
        for {
          statementAndBatches <- queriesStream.groupAdjacentBy(_.statement)
          (statement, statementBatches) = statementAndBatches
          batches = statementBatches.map(_.parameters)
          result <-
            Stream.eval(
              async.delay {
                pool.withConnection {implicit connection =>
                  Result(statement, batches.toVector.zip(prepareAndRun(statement, batches.toVector)))
                }
              }
            )
        } yield result

    override protected def logClass = classOf[com.rocketfuel.sdbc.base.jdbc.Batch]

    private def logRun(
      compiledStatement: CompiledStatement,
      batches: Vector[Parameters]
    ): Unit = {
      log.debug(s"""query "${compiledStatement.originalQueryText}, ${batches.size} batches"""")

      if (batches.isEmpty)
        log.warn("Executing a batch query without any batches.")
    }

  }

}
