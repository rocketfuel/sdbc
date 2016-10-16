package com.rocketfuel.sdbc.base.jdbc

import java.sql.SQLFeatureNotSupportedException
import com.rocketfuel.sdbc.base.Logging
import shapeless.ops.hlist._
import shapeless.ops.record.{Keys, Values}
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
    * @param parameters
    * @param batches
    */
  case class Batch private (
    statement: CompiledStatement,
    parameters: Parameters,
    batches: ParameterBatches
  ) extends ParameterizedQuery[Batch]
    with Executes {

    def addParameters(parameters: Parameters): Batch = {
      val newBatch = setParameters(parameters)

      Batch(
        statement,
        Parameters.empty,
        batches :+ newBatch
      )
    }

    def add(additionalParameter: (String, ParameterValue), additionalParameters: (String, ParameterValue)*): Batch = {
      addParameters(Map((additionalParameter +: additionalParameters): _*))
    }

    def addProduct[
      A,
      Repr <: HList,
      ReprKeys <: HList,
      ReprValues <: HList,
      MappedRepr <: HList
    ](t: A
    )(implicit genericA: LabelledGeneric.Aux[A, Repr],
      keys: Keys.Aux[Repr, ReprKeys],
      values: Values.Aux[Repr, ReprValues],
      valuesMapper: Mapper.Aux[ToParameterValue.type, ReprValues, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Batch = {
      addParameters(Parameters.product(t))
    }

    def addRecord[
      Repr <: HList,
      ReprKeys <: HList,
      ReprValues <: HList,
      MappedRepr <: HList
    ](t: Repr
    )(implicit keys: Keys.Aux[Repr, ReprKeys],
      values: Values.Aux[Repr, ReprValues],
      valuesMapper: Mapper.Aux[ToParameterValue.type, ReprValues, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Batch = {
      addParameters(Parameters.record(t))
    }

    def run()(implicit connection: Connection): IndexedSeq[Long] = {
      Batch.run(statement, batches)
    }

    override def execute()(implicit connection: Connection): Unit = {
      run()
    }

    override protected def subclassConstructor(
      parameters: Parameters
    ): Batch = {
      copy(parameters = parameters)
    }
  }

  object Batch
    extends Logging {

    def apply(
      queryText: String
    ): Batch = {
      val statement = CompiledStatement(queryText)
      apply(statement)
    }

    def apply(
      statement: CompiledStatement
    ): Batch = {
      Batch(
        statement = statement,
        parameters = Parameters.empty,
        batches = Vector.empty[Parameters]
      )
    }

    def literal(
      queryText: String
    ): Batch = {
      Batch(
        statement = CompiledStatement.literal(queryText),
        parameters = Parameters.empty,
        batches = Vector.empty[Parameters]
      )
    }

    def run(
      queryText: String,
      batches: ParameterBatches
    )(implicit connection: Connection): IndexedSeq[Long] = {
      val statement = CompiledStatement(queryText)
      run(statement, batches)
    }

    protected def prepare(
      compiledStatement: CompiledStatement,
      batches: ParameterBatches
    )(implicit connection: Connection
    ): PreparedStatement = {
      val prepared = connection.prepareStatement(compiledStatement.queryText)
      for (batch <- batches) {
        for ((name, value) <- batch) {
          for (index <- compiledStatement.parameterPositions(name)) {
            value.set(prepared, index)
          }
        }
        prepared.addBatch()
      }
      prepared
    }

    def run(
      compiledStatement: CompiledStatement,
      batches: Seq[Parameters]
    )(implicit connection: Connection
    ): IndexedSeq[Long] = {

      val prepared = prepare(compiledStatement, batches)

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

    private def logRun(
      compiledStatement: CompiledStatement,
      batches: Seq[Parameters]
    ): Unit = {
      logger.debug(s"""Executing batch of "${compiledStatement.originalQueryText}".""")

      if (batches.isEmpty) logger.warn("Executing a batch query without any batches.")
    }

  }

}
