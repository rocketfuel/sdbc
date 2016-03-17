package com.rocketfuel.sdbc.base

object CaseInsensitiveOrdering extends Ordering[String] {
  override def compare(
    x: String,
    y: String
  ): Int = {
    x.compareToIgnoreCase(y)
  }
}
