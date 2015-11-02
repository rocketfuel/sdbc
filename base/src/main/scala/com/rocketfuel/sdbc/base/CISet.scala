package com.rocketfuel.sdbc.base

import scala.collection.immutable.TreeSet

/**
 * Case insensitive set.
 */
object CISet {

  val empty = TreeSet.empty[String](CaseInsensitiveOrdering)

  def apply(elems: String*): TreeSet[String] = {
    TreeSet(elems: _*)(CaseInsensitiveOrdering)
  }

}
