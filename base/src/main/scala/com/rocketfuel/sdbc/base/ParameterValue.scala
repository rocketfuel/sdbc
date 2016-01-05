package com.rocketfuel.sdbc.base

trait ParameterValue {

  type Statement

  type Connection

  def prepareStatement(statement: String)(implicit connection: Connection): Statement

  protected def setNone(
    preparedStatement: Statement,
    parameterIndex: Int
  ): Statement

  protected def setOption[T](
    value: Option[T]
  )(implicit parameter: Parameter[T]
  ): (Statement, Int) => Statement = {
    value.map(parameter.set).getOrElse(setNone)
  }

  protected def prepare(
    queryText: String,
    parameterValues: Map[String, ParameterValue],
    parameterPositions: Map[String, Set[Int]]
  )(implicit connection: Connection
  ): Statement = {
    val preparedStatement = prepareStatement(queryText)

    bind(preparedStatement, parameterValues, parameterPositions)
  }

  protected def bind(
    preparedStatement: Statement,
    parameterValues: Map[String, ParameterValue],
    parameterPositions: Map[String, Set[Int]]
  ): Statement = {
    parameterValues.foldLeft(preparedStatement) {
      case (accum, (key, parameterValue)) =>
        val parameterIndices = parameterPositions(key)
        parameterIndices.foldLeft(accum) {
          case (accum2, index) =>
            parameterValue.set(accum, index)
        }
    }
  }

  trait Parameter[T] {
    val set: T => (Statement, Int) => Statement
  }

  /**
    * A type that is usable as a parameter, but it first must be converted.
    * @param baseParameter
    * @param conversion
    * @tparam T
    * @tparam JdbcType
    */
  class DerivedParameter[T, JdbcType](implicit baseParameter: Parameter[JdbcType], conversion: T => JdbcType)
    extends Parameter[T] {

    def toJdbcType(value: T): JdbcType = conversion(value)

    override val set: T => (Statement, Int) => Statement =
      (value: T) => {
        val converted = toJdbcType(value)
        (statement, parameterIndex) =>
          baseParameter.set(converted)(statement, parameterIndex)
      }

  }

  case class ParameterValue private[sdbc] (
    value: Option[Any],
    set: (Statement, Int) => Statement
  ) {
    override def equals(obj: scala.Any): Boolean = {
      obj match {
        case ParameterValue(otherValue, _) =>
          otherValue == value
        case otherwise =>
          false
      }
    }

    override def hashCode(): Int = {
      value.hashCode()
    }

    override def toString: String = {
      s"ParameterValue($value)"
    }
  }

  object ParameterValue {
    def apply[T](p: T)(implicit toParameterValue: T => ParameterValue): ParameterValue = {
      p
    }

    implicit def ofOption[T](p: Option[T])(implicit parameter: Parameter[T]): ParameterValue = {
      new ParameterValue(p, setOption(p))
    }

    implicit def of[T](p: T)(implicit parameter: Parameter[T]): ParameterValue = {
      ofOption[T](Some(p))
    }

    implicit def ofNone(p: None.type): ParameterValue = {
      new ParameterValue(None, setNone)
    }
  }

  type ParameterList = Seq[(String, ParameterValue)]

}
