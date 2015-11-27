package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base

trait Index extends base.Index[Row] {
  override protected def getColumnCount(row: Row): Int =
    row.getMetaData.getColumnCount

  override protected def getColumnIndex(row: Row, columnName: String): Int =
    row.columnIndexes(columnName)

  override protected def containsColumn(row: Row, columnName: String): Boolean =
    row.columnIndexes.contains(columnName)
}
