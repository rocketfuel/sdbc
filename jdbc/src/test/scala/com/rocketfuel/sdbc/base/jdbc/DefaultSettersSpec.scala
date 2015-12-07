package com.rocketfuel.sdbc.base.jdbc

import java.sql.PreparedStatement
import org.scalatest._

class DefaultSettersSpec
  extends FunSuite
  with Getter
  with CompositeGetter
  with ParameterValue
  with Row
  with MutableRow
  with ImmutableRow
  with Index
  with ResultSetImplicits
  with UpdatableRow
  with Updater
  with DefaultSetters
  with StringGetter {

  test("implicit Int conversion works") {
    assertCompiles("val _: Option[ParameterValue] = 3")
  }

  test("implicit Option[String] conversion works") {
    assertCompiles("val _: Option[ParameterValue] = Some(\"hello\")")
  }

  test("implicit scala.BigDecimal conversion works") {
    assertCompiles("val _: Option[ParameterValue] = BigDecimal(1)")
  }

  test("Row#get works") {
    assertCompiles("val row: Row = ???; val _ = row.get[String](???)")
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
  override def setAny(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Any): Unit = ???

}
