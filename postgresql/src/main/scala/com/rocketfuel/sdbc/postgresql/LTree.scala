package com.rocketfuel.sdbc.postgresql

import org.postgresql.util.PGobject

import scala.collection.immutable.Seq

/**
 * LTree class for use by the PostgreSQL JDBC driver.
 * It requires the class be a subclass of PGobject. It creates a new instance
 * using the zero-arg constructor, and then sets the value using setValue().
 * This mutable method of creating an object is not very scala-like, so we
 * supply custom apply and unapply methods.
 */
class LTree(
  private var path: Option[Seq[String]]
) extends PGobject()
  with collection.immutable.Iterable[String]
  with PartialFunction[Int, String] {

  def this() {
    this(None)
  }

  setType("ltree")

  path.foreach(LTree.validatePath)

  def getPath: Seq[String] = {
    path.
      getOrElse(throw new IllegalStateException("setValue or setPath must be called first"))
  }

  def setPath(path: Seq[String]): Unit = {
    if (this.path.isDefined)
      throw new IllegalStateException("setPath or setValue can not be called after setPath or setValue")

    LTree.validatePath(path)

    this.path = Some(path)
  }

  override def setValue(value: String): Unit = {
    setPath(value.split('.').toVector)
  }

  override def getValue: String = {
    getPath.mkString(".")
  }

  override def toString(): String = {
    getValue
  }

  def @>(that: LTree) = that.getPath.startsWith(this.getPath)

  def <@(that: LTree) = this.getPath.startsWith(that.getPath)

  override def iterator: Iterator[String] = getPath.iterator

  override def isDefinedAt(x: Int): Boolean = length > x

  def length: Int = getPath.length

  override def apply(idx: Int): String = getPath(idx)

  def ++(that: LTree): LTree = {
    val combined = new LTree
    combined.path = Some(this.getPath ++ that.getPath)
    combined
  }

  def ++(that: String): LTree = {
    LTree(this.getPath ++ LTree.fromString(that): _*)
  }

  def ++(that: Iterable[String]): LTree = {
    LTree.validatePath(that)
    val combined = new LTree
    combined.path = Some(this.getPath ++ that)
    combined
  }

  def :+(that: String): LTree = {
    LTree.validateNode(that)
    val combined = new LTree
    combined.path = Some(this.getPath :+ that)
    combined
  }

  def +:(that: String): LTree = {
    LTree.validateNode(that)
    val combined = new LTree
    combined.path = Some(that +: this.getPath)
    combined
  }

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case l: LTree => this.path == l.path
      case _ => false
    }
  }

  override def clone(): AnyRef = {
    val cloned = new LTree()
    cloned.setValue(this.getValue)
    cloned
  }
}

object LTree {

  def apply(path: String*): LTree = {
    apply(path.toVector)
  }

  def apply(path: Seq[String]): LTree = {
    new LTree(path = Some(path))
  }

  def unapply(ltree: LTree): Option[Seq[String]] = {
    Some(ltree.getPath)
  }

  def fromString(path: String): LTree = {
    val pathParts = path.split('.')
    apply(pathParts: _*)
  }

  def validatePath(nodes: Iterable[String]): Unit = {
    nodes.foreach(validateNode)
  }

  def validateNode(node: String): Unit = {
    if (node.isEmpty) {
      throw new IllegalArgumentException("LTree nodes can not be empty.")
    }

    if (node.contains('.')) {
      throw new IllegalArgumentException("LTree nodes can not contain '.'.")
    }
  }

}
