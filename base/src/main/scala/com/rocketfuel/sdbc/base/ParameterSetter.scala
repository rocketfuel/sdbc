package com.rocketfuel.sdbc.base

trait ParameterSetter {
  self: ParameterValue =>

  def set(
    preparedStatement: Statement,
    parameterIndex: Index,
    maybeParameter: Option[Any]
  ): Unit = {
    maybeParameter match {
      case None =>
        setNone(preparedStatement, parameterIndex)
      case Some(parameter) =>
        setAny(preparedStatement, parameterIndex, parameter)
    }
  }

  def setNone(
    preparedStatement: Statement,
    parameterIndex: Index
  ): Unit

  /**
    *
    * @param preparedStatement
    * @param parameterIndex
    * @param parameter The value to be set.
    * @param isParameter
    * @tparam T is a type understood by the DBMS driver.
    * @return
    */
  def setParameter[T](
    preparedStatement: Statement,
    parameterIndex: Int,
    parameter: T
  )(implicit isParameter: IsParameter[T]
  ): Unit = {
    isParameter.set(preparedStatement, parameterIndex, parameter)
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
  def setAny(
    preparedStatement: Statement,
    parameterIndex: Index,
    parameter: Any
  ): Unit

}
