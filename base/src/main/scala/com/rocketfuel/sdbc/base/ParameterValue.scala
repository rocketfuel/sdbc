package com.rocketfuel.sdbc.base

trait ParameterValue {

  type Statement

  type Index

  /**
    * ParameterValue holds values that ParameterSetter#set knows how to use. It is just
    * a wrapper for finding the IsParameter instance of a type to make sure that the
    * dbms knows how to handle it, and then ParameterValue is discarded, and the value is
    * stored directly in the parameter collection.
    *
    * @param value
    */
  case class ParameterValue private[sdbc] (value: Any)

  object ParameterValue {
    def fromIsParameter[T](t: T)(implicit isParameter: IsParameter[T]): ParameterValue = {
      ParameterValue(t)
    }
  }

  type ParameterList = Seq[(String, Option[ParameterValue])]

  trait IsParameter[T] {
    def set(preparedStatement: Statement, parameterIndex: Index, parameter: T): Unit
  }

  implicit class ParameterMethods[T](t: T)(implicit isParameter: IsParameter[T]) {
    def set(preparedStatement: Statement, parameterIndex: Index): Unit = {
      isParameter.set(preparedStatement, parameterIndex, t)
    }
  }

}
