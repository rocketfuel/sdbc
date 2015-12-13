package com.rocketfuel.sdbc.h2.implementation

import java.sql.PreparedStatement

import com.rocketfuel.sdbc.base.jdbc._

/**
 * Setters for H2 arrays.
 *
 * H2's jdbc driver does not support ResultSet#updateArray, so there are no updaters.
 */
private[sdbc] trait SeqParameter extends SeqGetter {
  self: ParameterValue
    with Updater
    with UpdatableRow
    with ParameterValue
    with MutableRow
    with Row
    with Getter =>

  implicit def SeqOptionParameter[T](implicit
    conversion: T => ParameterValue
  ): PrimaryParameter[Seq[Option[T]]] =
    new PrimaryParameter[Seq[Option[T]]] {
      override val toParameter: PartialFunction[Any, Any] = {
        PartialFunction.empty
      }
      override val setParameter: PartialFunction[Any, (Statement, ParameterIndex) => Statement] = {

      }
    }

  implicit def SeqOptionToOptionParameterValue[T](
    v: Seq[Option[T]]
  )(implicit conversion: T => ParameterValue
  ): ParameterValue = {
    ParameterValue(QSeq(v.map(_.map(conversion))))
  }

  implicit def SeqToOptionParameterValue[T](
    v: Seq[T]
  )(implicit conversion: T => ParameterValue
  ): ParameterValue = {
    v.map(Some.apply)
  }

  implicit val QSeqIsParameter: IsParameter[QSeq] = new IsParameter[QSeq] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: QSeq): Unit = {
      preparedStatement.setObject(parameterIndex, parameter.asJavaArray)
    }
  }

}
