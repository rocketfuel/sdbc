package com.rocketfuel.sdbc.base.jdbc

import java.sql.{SQLFeatureNotSupportedException, PreparedStatement}
import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.{Logging, CompiledStatement}
import shapeless.ops.hlist._
import shapeless.ops.record.{MapValues, Keys}
import shapeless.{LabelledGeneric, HList}

trait Batch {
  self: DBMS =>

  /**
    * Create and run a batch using a statement and a sequence of parameters.
    *
    * Batch contains two collections of parameters. One is a list of parameters for building a batch,
    * and a list of batches. Batches can be built using {@link #on} and finalized with {@link #addBatch},
    * or by passing parameters to {@link #addBatch}.
    *
    * @param statement
    * @param parameterValues
    * @param batches
    */
  case class Batch private[jdbc](
    statement: CompiledStatement,
    parameterValues: Map[String, ParameterValue],
    batches: Seq[Map[String, ParameterValue]]
  ) extends ParameterizedQuery[Batch] {

    protected def addBatch(additionalParameters: Parameters): Batch = {
      val newBatch = setParameters(additionalParameters.parameters)

      Batch(
        statement,
        Map.empty,
        batches :+ newBatch
      )
    }

    def addParameters(additionalParameters: Map[String, ParameterValue]): Batch = {
      addBatch(additionalParameters: Parameters)
    }

    def add(batchParameter: (String, ParameterValue), batchParameters: (String, ParameterValue)*): Batch = {
      addBatch(batchParameter +: batchParameters: Parameters)
    }

    def addProduct[
      P,
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](additionalParameters: P
    )(implicit genericA: LabelledGeneric.Aux[P, Repr],
      keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Batch = {
      addBatch(additionalParameters: Parameters)
    }

    def addRecord[
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](additionalParameters: Repr
    )(implicit keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Batch = {
      addBatch(additionalParameters: Parameters)
    }

    def run()(implicit connection: Connection): IndexedSeq[Long] = {
      Batch.run(statement, batches)
    }

    override protected def subclassConstructor(
      parameterValues: Map[String, ParameterValue]
    ): Batch = {
      copy(parameterValues = parameterValues)
    }
  }

  object Batch
    extends Logging {

    def apply(
      queryText: String
    ): Batch = {
      Batch(
        statement = CompiledStatement(queryText),
        parameterValues = Map.empty[String, ParameterValue],
        batches = Vector.empty[Map[String, ParameterValue]]
      )
    }

    def literal(
      queryText: String
    ): Batch = {
      Batch(
        statement = CompiledStatement.literal(queryText),
        parameterValues = Map.empty[String, ParameterValue],
        batches = Vector.empty[Map[String, ParameterValue]]
      )
    }

    def run(
      queryText: String,
      batches: Seq[Map[String, ParameterValue]]
    )(implicit connection: Connection): IndexedSeq[Long] = {
      val statement = CompiledStatement(queryText)
      run(statement, batches)
    }

    def run[
      P,
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](queryText: String,
      batches: Seq[P]
    )(implicit connection: Connection,
      genericA: LabelledGeneric.Aux[P, Repr],
      keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): IndexedSeq[Long] = {
      val statement = CompiledStatement(queryText)
      run(statement, batches.map(batch => (batch: Parameters).parameters))
    }

    def run[
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](queryText: String,
      batches: Seq[Repr]
    )(implicit connection: Connection,
      keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): IndexedSeq[Long] = {
      val statement = CompiledStatement(queryText)
      run(statement, batches.map(batch => (batch: Parameters).parameters))
    }

    protected def prepare(
      compiledStatement: CompiledStatement,
      batches: Seq[Map[String, ParameterValue]]
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

    private[jdbc] def run(
      compiledStatement: CompiledStatement,
      batches: Seq[Map[String, ParameterValue]]
    )(implicit connection: Connection
    ): IndexedSeq[Long] = {

      val prepared = prepare(compiledStatement, batches)

      logRun(compiledStatement)

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
      compiledStatement: CompiledStatement
    ): Unit = {
      logger.debug(s"""Executing batch of "${compiledStatement.originalQueryText}".""")
    }

  }

}
