package com.rocketfuel.sdbc.base.jdbc

import java.sql.{SQLFeatureNotSupportedException, PreparedStatement}
import com.rocketfuel.sdbc.base
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
    * @param parameterValueBatches
    */
  case class Batch private[jdbc](
    statement: base.CompiledStatement,
    parameterValues: Map[String, ParameterValue],
    parameterValueBatches: Seq[Map[String, ParameterValue]]
  ) extends base.Batch[Connection]
  with ParameterizedQuery[Batch]
  with base.Logging {

    def addBatch(parameterValues: (String, ParameterValue)*): Batch = {
      val newBatch = setParameters(parameterValues: _*)

      Batch(
        statement,
        Map.empty,
        parameterValueBatches :+ newBatch
      )
    }

    def addProductBatch[
      A,
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](param: A
    )(implicit genericA: LabelledGeneric.Aux[A, Repr],
      keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Batch = {
      val newBatch = setParameters(productParameters(param): _*)

      Batch(
        statement,
        Map.empty,
        parameterValueBatches :+ newBatch
      )
    }

    def addRecordBatch[
      Repr <: HList,
      MappedRepr <: HList,
      ReprKeys <: HList
    ](param: Repr
    )(implicit keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Batch = {
      val newBatch = setParameters(recordParameters(param): _*)

      Batch(
        statement,
        Map.empty,
        parameterValueBatches :+ newBatch
      )
    }

    protected def prepareBatches()(implicit connection: Connection): PreparedStatement = {
      val prepared = prepareStatement(queryText)
      for (batch <- parameterValueBatches) {
        bind(prepared, batch, parameterPositions)
        prepared.addBatch()
      }
      prepared
    }

    def seq()(implicit connection: Connection): IndexedSeq[Long] = {
      logger.debug( s"""Batching "$originalQueryText".""")
      val prepared = prepareBatches()
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

    override def iterator()(implicit connection: Connection): Iterator[Long] = {
      seq().toIterator
    }

    /**
      * Get the total count of updated or inserted rows.
      * @param connection
      * @return
      */
    def sum()(implicit connection: Connection): Long = {
      seq().sum
    }

    override protected def subclassConstructor(
      statement: base.CompiledStatement,
      parameterValues: Map[String, ParameterValue]
    ): Batch = {
      Batch(statement, parameterValues, Vector.empty)
    }
  }

  object Batch {
    def apply(
      queryText: String,
      hasParameters: Boolean = true
    ): Batch = {
      Batch(
        statement = base.CompiledStatement(queryText, hasParameters),
        parameterValues = Map.empty[String, ParameterValue],
        parameterValueBatches = Vector.empty[Map[String, ParameterValue]]
      )
    }
  }

}
