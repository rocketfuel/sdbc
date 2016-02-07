package com.rocketfuel.sdbc.base

import scala.collection.immutable.TreeMap

/**
 * Case insensitive map.
 */
object CIMap {

  def empty[B]: TreeMap[String, B] = TreeMap.empty[String, B](CaseInsensitiveOrdering)

  def apply[B](elems: (String, B)*): TreeMap[String, B] = {
    TreeMap(elems: _*)(CaseInsensitiveOrdering)
  }

}
