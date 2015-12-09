package com.rocketfuel.sdbc.base

trait ParameterValue {

  type Statement

  type Connection

  def prepareStatement(statement: String, connection: Connection): Statement

  type Index

  protected def prepare(
    queryText: String,
    parameterValues: Map[String, Option[Any]],
    parameterPositions: Map[String, Set[Index]]
  )(implicit connection: Connection
  ): Statement = {
    val preparedStatement = prepareStatement(queryText, connection)

    bind(preparedStatement, parameterValues, parameterPositions)

    preparedStatement
  }

  protected def bind(
    preparedStatement: Statement,
    parameterValues: Map[String, Option[Any]],
    parameterPositions: Map[String, Set[Index]]
  ): Unit = {
    parameterValues.foldLeft(preparedStatement) {
      case (accum, (key, maybeValue)) =>
        val parameterIndices = parameterPositions(key)
        parameterIndices.foldLeft(accum) {
          case (accum2, index) =>
            set(accum2, index, maybeValue)
        }
    }
  }

  private def set(
    preparedStatement: Statement,
    parameterIndex: Index,
    maybeParameter: Option[Any]
  ): Statement = {
    maybeParameter match {
      case None =>
        setNone(preparedStatement, parameterIndex)
      case Some(parameter) =>
        setSome(preparedStatement, parameterIndex, parameter)
    }
  }

  protected def setNone(
    preparedStatement: Statement,
    parameterIndex: Index
  ): Statement

  //null and the assignment in addSetSome is a hack to work around trait initialization order problems
  private var setSomeBuilder: PartialFunction[Any, (Statement, Index) => Statement] = null

  protected def addSetSome(f: PartialFunction[Any, (Statement, Index) => Statement]): Unit = {
    if (setSomeBuilder == null) setSomeBuilder = PartialFunction.empty
    setSomeBuilder = setSomeBuilder orElse f
  }

  /**
    * Pattern match to get the IsParameter instance for
    * a value, and then call setParameter.
    *
    * This method is to be implemented on a per-DBMS basis.
    * @param preparedStatement
    * @param parameterIndex
    * @param parameter
    */
  def setSome(
    preparedStatement: Statement,
    parameterIndex: Index,
    parameter: Any
  ): Statement = {
    setSomeBuilder(parameter)(preparedStatement, parameterIndex)
  }

  class ParameterValue private[sdbc] (val value: Option[Any])

  object ParameterValue {
    implicit def apply[T](p: Option[T])(implicit parameter: Parameter[T]): ParameterValue = {
      new ParameterValue(p)
    }

    implicit def apply[T](p: T)(implicit parameter: Parameter[T]): ParameterValue = {
      apply[T](Some(p))
    }

    def unapply(p: ParameterValue): Option[Any] = p.value

    implicit def asOption(p: ParameterValue): Option[Any] = {
      p.value
    }
  }

  type ParameterList = Seq[(String, ParameterValue)]

  sealed trait Parameter[T]

  object Parameter {
    def apply[T](implicit p: Parameter[T]): Parameter[T] = p

    private val emptyParameter: Parameter[Nothing] = new Parameter[Nothing] {}

    implicit def ofPrimaryParameter[T](implicit p: PrimaryParameter[T]): Parameter[T] = {
      emptyParameter.asInstanceOf[Parameter[T]]
    }

    implicit def ofSecondaryParameter[T](implicit p: SecondaryParameter[T]): Parameter[T] = {
      emptyParameter.asInstanceOf[Parameter[T]]
    }

    implicit def ofTertiaryParameter[T](implicit p: TertiaryParameter[T]): Parameter[T] = {
      emptyParameter.asInstanceOf[Parameter[T]]
    }

    implicit def ofQuaternaryParameter[T](implicit p: QuaternaryParameter[T]): Parameter[T] = {
      emptyParameter.asInstanceOf[Parameter[T]]
    }
  }

  trait PrimaryParameter[T] {
    val toParameter: PartialFunction[Any, Any]

    val setParameter: PartialFunction[Any, (Statement, Index) => Statement]

    addParameter(toParameter)
    addSetSome(setParameter)
  }

  /**
    * A dummy trait when adding more than one parameter at a time.
    * @tparam T
    */
  trait SecondaryParameter[T]

  trait TertiaryParameter[T]

  trait QuaternaryParameter[T]

  //null and the assignment in addParameter is a hack to work around trait initialization order problems
  private var toParameterBuilder: PartialFunction[Any, Any] = null

  protected def addParameter(f: PartialFunction[Any, Any]): Unit = {
    if (toParameterBuilder == null) toParameterBuilder = PartialFunction.empty
    toParameterBuilder = toParameterBuilder orElse f
  }

  /**
    * Takes a value and converts it into a type that setParameter expects
    * @return
    */
  def toParameter: PartialFunction[Any, Any] = toParameterBuilder

}
