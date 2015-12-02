package com.rocketfuel.sdbc.sqlserver

case class HierarchyId(path: HierarchyNode*) {
  override def toString: String = {
    if (path.isEmpty) {
      "/"
    } else {
      path.map(_.toString).mkString("/", "/", "/")
    }
  }
}

object HierarchyId {
  def fromString(path: String): HierarchyId = {
    if (path.isEmpty || path.charAt(0) != '/')
      throw new IllegalArgumentException("HierarchyId must start with '/'.")

    if (path.last != '/')
      throw new IllegalArgumentException("HierarchyId must end with '/'.")

    //"".split('/') => Array("")
    //"/".split('/') => Array()
    //Weird, huh?

    path match {
      case "/" => HierarchyId()
      case _ =>
        //There is an empty string at the beginning due to the leading '/',
        //so be sure to drop it.
        val pathParts = path.split('/').drop(1).map(HierarchyNode.fromString)
        HierarchyId(pathParts: _*)
    }
  }

  val empty = HierarchyId()
}
