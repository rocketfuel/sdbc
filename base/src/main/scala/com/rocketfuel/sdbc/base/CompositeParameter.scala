package com.rocketfuel.sdbc.base

import shapeless._
import shapeless.ops.product.ToRecord
import shapeless.syntax.std.product._
import shapeless.record._

case class CompositeParameter(parameters: Seq[(String, ParameterValue)])

object CompositeParameter {

  object ToParameterValue extends Poly {
    implicit def valueToParameter[A](value: A)(implicit converter: A => ParameterValue) = {
      use((parameter: A) => converter(parameter))
    }
  }

  implicit def apply[A <: Product](parameters: A): CompositeParameter = {
    val mappableParams = parameters.toRecord//parameters.toRecord

    val mappedParams = mappableParams.mapValues(ToParameterValue)

    val paramsMap = mappedParams.toMap[String, ParameterValue]

    CompositeParameter(paramsMap.toSeq: _*)
  }

}
