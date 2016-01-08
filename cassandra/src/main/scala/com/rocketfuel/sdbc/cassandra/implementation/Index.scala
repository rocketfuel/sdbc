package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core
import com.rocketfuel.sdbc.base

trait Index extends base.Index {

  override type Row = core.Row

  override def getColumnCount(row: Row): Int = row.getColumnDefinitions.size()

  override def getColumnIndex(row: Row, columnName: String): Int = {
    row.getColumnDefinitions.getIndexOf(columnName) match {
      case -1 => throw new NoSuchElementException("key not found: " + columnName)
      case columnIndex => columnIndex
    }
  }

  override def containsColumn(row: Row, columnName: String): Boolean = {
    row.getColumnDefinitions.contains(columnName)
  }

}
