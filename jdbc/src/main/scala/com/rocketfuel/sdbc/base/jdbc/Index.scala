package com.rocketfuel.sdbc.base.jdbc

trait Index extends PartialFunction[Row, Int]

object Index {

  implicit def apply(columnIndex: Int): Index = IntIndex(columnIndex)

  implicit def apply(columnLabel: String): Index = StringIndex(columnLabel)

}

case class IntIndex(columnIndex: Int) extends Index {
  override def isDefinedAt(row: Row): Boolean = {
    columnIndex < row.getMetaData.getColumnCount
  }

  override def apply(row: Row): Int = columnIndex

  def +(i: Int): Index = {
    i + columnIndex
  }
}

case class StringIndex(columnLabel: String) extends Index {
  override def isDefinedAt(row: Row): Boolean = {
    row.columnIndexes.contains(columnLabel)
  }

  override def apply(row: Row): Int = {
    row.columnIndexes(columnLabel)
  }
}
