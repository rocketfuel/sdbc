package com.rocketfuel.sdbc.cassandra.datastax.implementation

import com.datastax.driver.core.Row

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

private[sdbc] case class IntIndex(columnIndex: Int) extends Index {
  override def isDefinedAt(row: Row): Boolean = {
    columnIndex < row.getColumnDefinitions.size()
  }

  override def apply(row: Row): Int = {
    columnIndex
  }
}

private[sdbc] case class StringIndex(columnLabel: String) extends Index {
  override def isDefinedAt(row: Row): Boolean = {
    row.getColumnDefinitions.contains(columnLabel)
  }

  override def apply(row: Row): Int = {
    row.getColumnDefinitions.getIndexOf(columnLabel) match {
      case -1 => throw new NoSuchElementException("key not found: " + columnLabel)
      case columnIndex => columnIndex
    }
  }
}

private[sdbc] case class AdditiveIndex(ix: Index, toAdd: Int) extends Index {
  override def isDefinedAt(row: Row): Boolean = {
    ix.isDefinedAt(row) && {
      val baseIx = ix(row)
      baseIx + toAdd < row.getColumnDefinitions.size()
    }
  }

  override def apply(row: Row): Int = {
    ix(row) + toAdd
  }
}
