package com.rocketfuel.sdbc.base

trait Index {

  type Row

  protected def getColumnCount(row: Row): Int

  protected def containsColumn(row: Row, columnName: String): Boolean

  protected def getColumnIndex(row: Row, columnName: String): Int

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
      columnIndex < getColumnCount(row)
    }

    override def apply(row: Row): Int = columnIndex
  }

  case class StringIndex(columnLabel: String) extends Index {
    override def isDefinedAt(row: Row): Boolean = {
      containsColumn(row, columnLabel)
    }

    override def apply(row: Row): Int = {
      getColumnIndex(row, columnLabel)
    }
  }

  case class AdditiveIndex(ix: Index, toAdd: Int) extends Index {
    override def isDefinedAt(row: Row): Boolean = {
      ix.isDefinedAt(row) && {
        val baseIx = ix(row)
        baseIx + toAdd < getColumnCount(row)
      }
    }

    override def apply(row: Row): Int = {
      ix(row) + toAdd
    }
  }

}
