package com.rocketfuel.sdbc.h2

/**
 * Serialized tells the H2 client to send the parameter
 * as a serialized object instead of trying the other implicit conversions
 * to ParameterValue.
 */
@SerialVersionUID(-3881436119431769328L)
case class Serialized(
  value: AnyRef with java.io.Serializable
)
