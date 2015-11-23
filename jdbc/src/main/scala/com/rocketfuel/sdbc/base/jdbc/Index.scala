package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base

trait Index extends base.Index[Row] {
  override def getColumnCount(row: Row): Int =
    row.getMetaData.getColumnCount

  override def getColumnIndex(row: Row, columnName: String): Int =
    row.columnIndexes(columnName)

  override def containsColumn(row: Row, columnName: String): Boolean =
    row.columnIndexes.contains(columnName)
}
