package com.rocketfuel.sdbc.sqlserver

case class HierarchyNode(start: Int, path: Int*) {
  override def toString: String = {
    (start +: path).mkString(".")
  }
}

object HierarchyNode {
  implicit def fromString(path: String): HierarchyNode = {
    val pathParts = path.split('.').map(_.toInt)
    apply(pathParts.head, pathParts.tail: _*)
  }

  implicit def fromInt(i: Int): HierarchyNode = {
    HierarchyNode(i)
  }

  implicit def fromInts(path: Seq[Int]): HierarchyNode = {
    HierarchyNode(path.head, path.tail: _*)
  }

}
