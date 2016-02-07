package com.rocketfuel.sdbc.base

trait Index {

  trait RowIndexOps {

    def columnIndexes: Map[String, Int]

    def columnNames: IndexedSeq[String]

    def columnCount: Int

  }

  trait Index extends PartialFunction[RowIndexOps, Int] {

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
    override def isDefinedAt(row: RowIndexOps): Boolean = {
      columnIndex < row.columnCount
    }

    override def apply(row: RowIndexOps): Int = columnIndex
  }

  case class StringIndex(columnLabel: String) extends Index {
    override def isDefinedAt(row: RowIndexOps): Boolean = {
      row.columnNames.contains(columnLabel)
    }

    override def apply(row: RowIndexOps): Int = {
      row.columnIndexes(columnLabel)
    }
  }

  case class AdditiveIndex(ix: Index, toAdd: Int) extends Index {
    override def isDefinedAt(row: RowIndexOps): Boolean = {
      ix.isDefinedAt(row) && {
        val baseIx = ix(row)
        baseIx + toAdd < row.columnCount
      }
    }

    override def apply(row: RowIndexOps): Int = {
      ix(row) + toAdd
    }
  }

}
