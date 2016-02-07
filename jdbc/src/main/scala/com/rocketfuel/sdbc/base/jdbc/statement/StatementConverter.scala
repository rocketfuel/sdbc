package com.rocketfuel.sdbc.base.jdbc.statement

import com.rocketfuel.sdbc.base.jdbc.DBMS
import java.sql.SQLFeatureNotSupportedException
import shapeless.{HNil, ::, HList}

trait StatementConverter {
  self: DBMS =>

  sealed trait StatementExecutionResult[T] {
    val get: T
  }

  case class IteratorResult[A](get: Iterator[A]) extends StatementExecutionResult[Iterator[A]]

  case object EmptyResult extends StatementExecutionResult[Unit] {
    override val get: Unit = ()
  }

  case class UpdateResult(get: Int) extends StatementExecutionResult[Int]

  case class LargeUpdateResult(get: Long) extends StatementExecutionResult[Long]

  case class CompoundResult[T](get: T) extends StatementExecutionResult[T]

  //  @implicitNotFound("Import a DBMS or define a function from Statement to A.")
  trait StatementConverter[A] extends (Statement => A)

  object StatementConverter extends LowerPriorityStatementConverterImplicits {

    def apply[A](implicit statementConverter: StatementConverter[A]): StatementConverter[A] = statementConverter

    implicit def ofFunction[A](f: Statement => A): StatementConverter[A] =
      new StatementConverter[A] {
        override def apply(v1: Statement): A = f(v1)
      }

    implicit val long: StatementConverter[Long] = {
      (statement: Statement) =>
        try {
          statement.getLargeUpdateCount
        } catch {
          case _: UnsupportedOperationException |
               SQLFeatureNotSupportedException =>
            statement.getUpdateCount.toLong
        }
    }

    implicit val int: StatementConverter[Int] = {
      (statement: Statement) =>
        statement.getUpdateCount
    }

    implicit val unit: StatementConverter[Unit] = {
      (statement: Statement) => ()
    }

    implicit def iterator[R >: ImmutableRow, A](implicit rowConverter: RowConverter[R, A]): StatementConverter[Iterator[A]] = {
      (statement: Statement) =>
        ImmutableRow.iterator(statement.getResultSet).map(rowConverter)
    }

  }

  /**
    * Automatically generated statement converters are to be used
    * only if there isn't an explicit statement converter.
    */
  trait LowerPriorityStatementConverterImplicits {
    implicit def product[H, T <: HList](implicit
      H: StatementConverter[H],
      T: StatementConverter[T]
    ): CompositeGetter[HTRowUpperBound, H :: T] =
      new CompositeGetter[HTRowUpperBound, H :: T] {
        override def apply(row: HTRowUpperBound, ix: Index): H :: T = {
          val head = H(row.asInstanceOf[HRow], ix)
          val tail = T(row.asInstanceOf[TRow], ix + H.length)
          head :: tail
        }

        override val length: Int = H.length + T.length
      }

    implicit def emptyProduct[R]: StatementC =
      new CompositeGetter[Row, HNil] {

        override def apply(v1: Row, v2: Index): HNil = {
          HNil
        }

        override val length: Int = 0
      }
  }

}
