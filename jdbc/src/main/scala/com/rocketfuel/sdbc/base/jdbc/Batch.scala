package com.rocketfuel.sdbc.base.jdbc

import java.sql.PreparedStatement
import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.base.{Logging, CompiledStatement}

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
 * @param parameterSetter
 */
case class Batch private [jdbc] (
  statement: CompiledStatement,
  parameterValues: Map[String, Option[Any]],
  parameterValueBatches: Seq[Map[String, Option[Any]]]
)(implicit parameterSetter: ParameterSetter
) extends base.Batch[Connection]
  with ParameterizedQuery[Batch]
  with Logging {

  def addBatch(parameterValues: (String, Option[ParameterValue])*): Batch = {
    val newBatch = setParameters(parameterValues: _*)

    Batch(
      statement,
      Map.empty,
      parameterValueBatches :+ newBatch
    )
  }

  protected def prepare()(implicit connection: Connection): PreparedStatement = {
    val prepared = connection.prepareStatement(queryText)
    for (batch <- parameterValueBatches) {
      for ((name, maybeValue) <- batch) {
        for (index <- parameterPositions(name)) {
          maybeValue match {
            case None =>
              parameterSetter.setNone(prepared, index + 1)
            case Some(value) =>
              parameterSetter.setAny(prepared, index + 1, value)
          }
        }
      }
      prepared.addBatch()
    }
    prepared
  }

  def seq()(implicit connection: Connection): IndexedSeq[Long] = {
    logger.debug(s"""Batching "$originalQueryText".""")
    val prepared = prepare()
    val result = try {
      prepared.executeLargeBatch()
    } catch {
      case e: UnsupportedOperationException =>
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
    statement: CompiledStatement,
    parameterValues: Map[String, Option[Any]]
  ): Batch = {
    Batch(statement, parameterValues, Vector.empty)
  }
}

object Batch {
  def apply(
    queryText: String,
    hasParameters: Boolean = true
  )(implicit parameterSetter: ParameterSetter
  ): Batch = {
    Batch(
      statement = CompiledStatement(queryText, hasParameters),
      parameterValues = Map.empty[String, Option[Any]],
      parameterValueBatches = Vector.empty[Map[String, Option[Any]]]
    )
  }
}
