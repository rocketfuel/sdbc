package com.rocketfuel.sdbc.base.jdbc

trait Index extends PartialFunction[Row, Int] {
  def +(toAdd: Int): Index = {
    this match {
      case AdditiveIndex(ix, originalToAdd) =>
        AdditiveIndex(ix, originalToAdd + toAdd)
      case otherwise =>
        AdditiveIndex(this, toAdd)
    }
  }

  def -(toSubtract: Int): Index = {
    this + (-toSubtract)
  }
}

object Index {

  implicit def apply(columnIndex: Int): Index = IntIndex(columnIndex)

  implicit def apply(columnLabel: String): Index = StringIndex(columnLabel)

}

case class IntIndex(columnIndex: Int) extends Index {
  override def isDefinedAt(row: Row): Boolean = {
    columnIndex < row.getMetaData.getColumnCount
  }

  override def apply(row: Row): Int = columnIndex
}

case class StringIndex(columnLabel: String) extends Index {
  override def isDefinedAt(row: Row): Boolean = {
    row.columnIndexes.contains(columnLabel)
  }

  override def apply(row: Row): Int = {
    row.columnIndexes(columnLabel)
  }
}

case class AdditiveIndex(ix: Index, toAdd: Int) extends Index {
  override def isDefinedAt(row: Row): Boolean = {
    ix.isDefinedAt(row) && {
      val baseIx = ix(row)
      baseIx + toAdd < row.getMetaData.getColumnCount
    }
  }

  override def apply(row: Row): Int = {
    ix(row) + toAdd
  }
}
