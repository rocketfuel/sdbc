package com.wda.sdbc.cassandra

import java.sql.SQLException

import com.datastax.driver.core.{Row => CRow}

case class IntIndex(columnIndex: Int) extends Index {
  override def isDefinedAt(row: CRow): Boolean = {
    columnIndex < row.getColumnDefinitions.size()
  }

  override def apply(row: CRow): Int = {
    columnIndex
  }
}

case class StringIndex(columnLabel: String) extends Index {
  override def isDefinedAt(row: CRow): Boolean = {
    row.getColumnDefinitions.contains(columnLabel)
  }

  override def apply(row: CRow): Int = {
    row.getColumnDefinitions.getIndexOf(columnLabel) match {
      case -1 => throw new SQLException(s"Column $columnLabel does not exist in the row.")
      case columnIndex => columnIndex
    }
  }
}
