package com.rocketfuel.sdbc.base.jdbc.statement

import com.rocketfuel.sdbc.base.jdbc.DBMS
import java.sql.{PreparedStatement, _}
import shapeless.labelled._
import shapeless.{::, Generic, HList, HNil, Lazy}

trait MultiResultConverter {
  self: DBMS =>

  trait MultiResultConverter[A] extends (PreparedStatement => A)

  object MultiResultConverter extends LowerPriorityMultiResultConverter {

    def apply[A](implicit statementConverter: MultiResultConverter[A]): MultiResultConverter[A] =
      statementConverter

    implicit def ofFunction[A](f: PreparedStatement => A): MultiResultConverter[A] =
      new MultiResultConverter[A] {
        override def apply(v1: PreparedStatement): A = f(v1)
      }

    implicit val results: MultiResultConverter[ResultSet] = {
      (v1: PreparedStatement) => {
        Option(v1.getResultSet()).get
      }
    }

    implicit val immutableResults: MultiResultConverter[CloseableIterator[ImmutableRow]] = {
      (v1: PreparedStatement) => {
        ImmutableRow.iterator(results(v1))
      }
    }

    implicit val connectedResults: MultiResultConverter[CloseableIterator[ConnectedRow]] = {
      (v1: PreparedStatement) => {
        ConnectedRow.iterator(results(v1))
      }
    }

    implicit val unit: MultiResultConverter[Unit] =
      new MultiResultConverter[Unit] {
        override def apply(v1: PreparedStatement): Unit = ()
      }

    implicit val update: MultiResultConverter[QueryResult.UpdateCount] = {
      (v1: PreparedStatement) =>
        val count = try {
          v1.getLargeUpdateCount
        } catch {
          case _: UnsupportedOperationException |
               _: SQLFeatureNotSupportedException =>
            v1.getUpdateCount.toLong
        }
        if (count == -1) None.get else QueryResult.UpdateCount(count)
    }

    implicit def convertedRowIterator[A](implicit
      converter: RowConverter[A]
    ): MultiResultConverter[Iterator[A]] = {
      (v1: PreparedStatement) =>
        connectedResults(v1).mapCloseable(converter)
    }

    implicit def convertedRowVector[A](implicit
      converter: RowConverter[A]
    ): MultiResultConverter[Vector[A]] = {
      (v1: PreparedStatement) =>
        val i = connectedResults(v1)
        try i.map(converter).toVector
        finally i.close()
    }

    implicit def convertedRowOption[A](implicit
      converter: RowConverter[A]
    ): MultiResultConverter[Option[A]] = {
      (v1: PreparedStatement) =>
        val i = connectedResults(v1)
        try i.map(converter).toStream.headOption
        finally i.close()
    }

    implicit def convertedRowSingleton[
      R <: Row,
      A
    ](implicit converter: RowConverter[A]
    ): MultiResultConverter[A] =  {
      (v1: PreparedStatement) =>
        val i = connectedResults(v1)
        try i.map(converter).toStream.head
        finally i.close()
    }

    implicit def recordComposite[
      H,
      T <: HList,
      K <: Symbol
    ](implicit
      H: MultiResultConverter[H],
      T: MultiResultConverter[T]
    ): MultiResultConverter[FieldType[K, H] :: T] =
      new MultiResultConverter[FieldType[K, H] :: T] {

        override def apply(v1: PreparedStatement): ::[FieldType[K, H], T] = {
          val head = H(v1)
          val tail = T(v1)

          field[K](head) :: tail
        }
      }

  }

  trait LowerPriorityMultiResultConverter {

    implicit val emptyProduct: MultiResultConverter[HNil] =
      new MultiResultConverter[HNil] {
        override def apply(v1: PreparedStatement): HNil = HNil
      }

    implicit def product[H, T <: HList](implicit
      H: MultiResultConverter[H],
      T: MultiResultConverter[T]
    ): MultiResultConverter[H :: T] = {
      new MultiResultConverter[H :: T] {
        override def apply(v1: PreparedStatement): H :: T = {
          val h = H(v1)
          v1.getMoreResults(Statement.KEEP_CURRENT_RESULT)
          val t = T(v1)
          h :: t
        }
      }
    }

    implicit def generic[F, G](implicit
      gen: Generic.Aux[F, G],
      G: Lazy[MultiResultConverter[G]]
    ): MultiResultConverter[F] =
      new MultiResultConverter[F] {
        override def apply(v1: PreparedStatement): F = {
          gen.from(G.value(v1))
        }
      }

  }

}
