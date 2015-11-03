package com.rocketfuel.sdbc.cassandra.datastax.implementation

import com.datastax.driver.core.Row

trait Index extends PartialFunction[Row, Int]

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

  def +(i: Int): Index = {
    columnIndex + i
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
