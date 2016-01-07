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

  trait Parameter[-A] {
    val set: A => (Statement, Int) => Statement
  }

  object Parameter {
    implicit def apply[A](set0: A => (Statement, Int) => Statement): Parameter[A] = new Parameter[A] {
      override val set: (A) => (Statement, Int) => Statement = set0
    }
  }

  trait DerivedParameter[-A] extends Parameter[A] {

    type B

    val conversion: A => B
    val baseParameter: Parameter[B]

    override val set: A => (Statement, Int) => Statement = {
      (value: A) => {
        val converted = conversion(value)
        (statement, parameterIndex) =>
          baseParameter.set(converted)(statement, parameterIndex)
      }
    }

  }

  object DerivedParameter {
    type Aux[A, B0] = DerivedParameter[A] { type B = B0 }

    implicit def apply[A, B0](implicit conversion0: A => B0, baseParameter0: Parameter[B0]): DerivedParameter[A] =
      new DerivedParameter[A] {
        type B = B0
        override val conversion: A => B = conversion0
        override val baseParameter: Parameter[B] = baseParameter0
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
      ParameterValue(p, setOption(p))
    }

    implicit def of[T](p: T)(implicit parameter: Parameter[T]): ParameterValue = {
      ofOption[T](Some(p))
    }

    implicit def ofNone(p: None.type): ParameterValue = {
      empty
    }

    val empty = ParameterValue(None, setNone)
  }

  type ParameterList = Seq[(String, ParameterValue)]

}
