package com.rocketfuel.sdbc.base.jdbc.statement

import com.rocketfuel.sdbc.base.jdbc.DBMS
import java.sql._
import scala.collection.immutable
import shapeless._

trait StatementConverter {
  self: DBMS =>

  trait StatementConverter[A, B] extends (Statement => A)

  object StatementConverter extends LowerPriorityStatementConverter {

    implicit val unit: StatementConverter[Unit, Result.Unit] = {
      (v1: Statement) =>
        new Result.Unit(v1)
    }

    implicit val update: StatementConverter[Long, Result.UpdateCount] = {
      (v1: Statement) =>
        new Result.UpdateCount(v1)
    }

    implicit def iterator[A](
      implicit converter: RowConverter[Row, A]
    ): StatementConverter[Iterator[A], Result.Iterator[A]] = {
      (v1: Statement) =>
        new Result.Iterator[A](v1)
    }

    implicit val immutableIterator: StatementConverter[Iterator[ImmutableRow], Result.ImmutableIterator] = ofFunction {
      (v1: Statement) =>
        new Result.ImmutableIterator(v1)
    }

    implicit def convertedRowSeq[A](
      implicit converter: RowConverter[Row, A]
    ): StatementConverter[immutable.Seq[A], Result.Seq[A]] = ofFunction {
      (v1: Statement) =>
        new Result.Seq[A](v1)
    }

    implicit def convertedRowOption[A](
      implicit converter: RowConverter[Row, A]
    ): StatementConverter[Option[A], Result.Option[A]] = ofFunction {
      (v1: Statement) =>
        new Result.Option[A](v1)
    }

    implicit def convertedRowSingleton[A](
      implicit converter: RowConverter[Row, A]
    ): StatementConverter[A, Result.Single[A]] =  ofFunction {
      (v1: Statement) =>
        new Result.Single(v1)
    }

    implicit def `try`[A](
      implicit result: Result[A]
    ): StatementConverter[util.Try[A], Result.Try[A]] = ofFunction {
      (v1: Statement) =>
        new Result.Try[A](result)
    }

    implicit def cons[Head, Tail <: HList](
      implicit H: Result[Head],
      T: Result[Tail]
    ): StatementConverter[Head :: Tail, Result.Cons[Head, Tail]] = ofFunction {
      (v1: Statement) =>
        new Result.Cons(H, T)
    }

  }

  trait LowerPriorityStatementConverter {

    def apply[InnerResult, OuterResult <: Result[InnerResult]](implicit statementConverter: StatementConverter[InnerResult, OuterResult]): StatementConverter[InnerResult, OuterResult] =
      statementConverter

    protected def ofFunction[InnerResult, OuterResult <: Result[InnerResult]](f: Statement => InnerResult): StatementConverter[InnerResult, OuterResult] =
      new StatementConverter[InnerResult, OuterResult] {
        override def apply(s: Statement): InnerResult = f(s)
      }

    implicit val hnil: StatementConverter[HNil, Result.HNil] =
      ofFunction((v1: Statement) => Result.HNil)

    implicit def product[
      Head,
      Tail <: HList
    ](implicit H: StatementConverter[Head, Result[Head]],
      T: StatementConverter[Tail, Result[Tail]]
    ): StatementConverter[Head :: Tail, Result.Cons[Head, Tail]] = ofFunction {
      (v1: Statement) =>
        new Result.Cons[Head, Tail](H.toResult(v1), T.toResult(v1))
    }

    implicit def generic[F, G](implicit
      gen: Generic.Aux[F, G],
      G: Lazy[StatementConverter[G, Result[G]]]
    ): StatementConverter[F, Result[F]] = {
      new StatementConverter[F, Result[F]] {
        override def toResult(s: Statement): Result.Identity[F] = {
          new Result.Identity[F](gen.from(G.value(s)))
        }
      }
    }
  }

  trait Result[R] {
    parent =>

    def run(): R

    def map[R1](f: R => R1): Result[R1] = new Result[R1] {
      override def run(): R1 = f(parent.run())
    }

    def flatMap[R1](f: R => Result[R1]): Result[R1] = new Result[R1] {
      override def run(): R1 = f(parent.run()).run()
    }
  }

  object Result {

    class Unit private[StatementConverter](statement: Statement) extends Result[scala.Unit] {
      type Result = scala.Unit
      
      override def run(): scala.Unit = {
        statement.getMoreResults()
      }
    }

    class Single[A] private[StatementConverter](statement: Statement)(implicit converter: RowConverter[Row, A]) extends Result[A] {
      override def run(): A = {
        new Option(statement).run().get
      }
    }

    class Option[A] private[StatementConverter](statement: Statement)(implicit converter: RowConverter[Row, A]) extends Result[scala.Option[A]] {
      override def run(): scala.Option[A] = {
        try {
          val results = statement.getResultSet()
          if (results.next()) Some(converter(UpdatableRow(results)))
          else None
        } finally statement.getMoreResults()
      }
    }

    class Iterator[A] private[StatementConverter](statement: Statement)(implicit rowConverter: RowConverter[Row, A]) extends Result[collection.Iterator[A]] {
      override def run(): collection.Iterator[A] = {
        val rs = new ResultSet(statement).run()
        UpdatableRow.iterator(rs).map(rowConverter)
      }
    }

    class ImmutableIterator private[StatementConverter](statement: Statement) extends Result[collection.Iterator[ImmutableRow]] {
      override def run(): collection.Iterator[ImmutableRow] = {
        val rs = new ResultSet(statement).run()
        ImmutableRow.iterator(rs)
      }
    }

    class Seq[A] private[StatementConverter](statement: Statement)(implicit converter: RowConverter[Row, A]) extends Result[immutable.Seq[A]] {
      override def run(): immutable.Seq[A] = {
        val rs = statement.getResultSet()
        try {
          UpdatableRow.iterator(rs).map(converter).toVector
        } finally statement.getMoreResults()
      }
    }

    class UpdateCount private[StatementConverter](statement: Statement) extends Result[Long] {
      override def run(): Long = {
        try {
          val count = try {
            statement.getLargeUpdateCount
          } catch {
            case _: UnsupportedOperationException |
                 _: SQLFeatureNotSupportedException =>
              statement.getUpdateCount.toLong
          }
          if (count == -1) None.get else count
        } finally statement.getMoreResults()
      }
    }

    class ResultSet(statement: Statement) extends Result[java.sql.ResultSet] {
      /**
        * @throws NoSuchElementException when there are no more results or the result is an update count.
        */
      override def run(): java.sql.ResultSet = {
        try {
          Option(statement.getResultSet()).get
        } finally statement.getMoreResults(Statement.KEEP_CURRENT_RESULT)
      }
    }

    class Try[A](result: Result[A]) extends Result[util.Try[A]] {
      override def run(): util.Try[A] = {
        util.Try(result.run())
      }
    }

    class Cons[Head, Tail <: HList](
      H: Result[Head],
      T: Result[Tail]
    ) extends Result[Head :: Tail] {
      override def run(): Head :: Tail = {
        H.run() :: T.run()
      }
    }

    class HNil extends Result[shapeless.HNil] {
      override def run(): shapeless.HNil = shapeless.HNil
    }

    object HNil extends HNil

    private[jdbc] class Identity[A](result: A) extends Result[A] {
      override def run(): A = result
    }

  }

}
