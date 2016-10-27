package com.rocketfuel.sdbc.base

private sealed trait QueryPart {
  /**
    * The code points that make up this part of the query.
    */
  val value: Vector[Int]

  override def toString: String = new String(value.toArray.flatMap(Character.toChars(_)))
}

private case class Parameter(value: Vector[Int]) extends QueryPart

private case class QueryText(value: Vector[Int]) extends QueryPart
