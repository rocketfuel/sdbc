package com.rocketfuel.sdbc.base.jdbc

import java.sql.SQLException

trait Index extends PartialFunction[Row, Int]

object Index {

  implicit def apply(columnIndex: Int): Index = new Index {
    override def isDefinedAt(row: Row): Boolean = {
      columnIndex < row.getMetaData.getColumnCount
    }

    override def apply(row: Row): Int = columnIndex
  }

  implicit def apply(columnLabel: String): Index = new Index {
    override def isDefinedAt(row: Row): Boolean = {
      try {
        apply(row) >= 0
      } catch {
        case e: SQLException =>
          false
      }
    }

    override def apply(row: Row): Int = {
      row.columnIndexes(columnLabel)
    }
  }

}
