package com.rocketfuel.sdbc.h2.implementation

import java.sql.PreparedStatement
import reflect.runtime.universe.TypeTag
import com.rocketfuel.sdbc.base.jdbc._

/**
 * Setters for H2 arrays.
 *
 * H2's jdbc driver does not support ResultSet#updateArray, so there are no updaters.
 */
private[sdbc] trait SeqParameter extends SeqGetter {
  self: H2 =>

  def typeName[T](implicit tpe: TypeTag): String = {

  }

  implicit def SeqOptionParameter[T](implicit
    conversion: T => ParameterValue
  ): Parameter[Seq[Option[T]]] =
    new Parameter[Seq[Option[T]]] {
      override val set: (Seq[Option[T]]) => (PreparedStatement, Int) => PreparedStatement = {
        seq => (statement, index) =>
          val seqParameter = seq.map(_.map(conversion))
          val array = statement.getConnection.createArrayOf()
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

}
