package com.example

package object jsondbms {

  import argonaut._
  import com.rocketfuel.sdbc.base.CloseableIterator
  import fs2.{Pipe, Sink, Stream}
  import fs2.util.Async
  import java.io.Closeable

  trait Connection extends Closeable

  trait ConnectionPool {
    def get(): Connection
  }

  object Select {

    def iterator(query: Json)(implicit connection: Connection): CloseableIterator[Json] = ???

    def iterator[
      Query,
      Result
    ](query: Query
    )(implicit queryEnc: EncodeJson[Query],
      resultDec: DecodeJson[Result],
      connection: Connection
    ): CloseableIterator[Result] =
      for {
        result <- iterator(queryEnc(query))
      } yield resultDec.decodeJson(result).value.get

    def stream[
      F[_],
      Query,
      Result
    ](query: Query
    )(implicit queryEnc: EncodeJson[Query],
      resultDec: DecodeJson[Result],
      pool: ConnectionPool,
      a: Async[F]
    ): Stream[F, Result] = {
      Stream.bracket[F, Connection, Result](
        a.delay(pool.get())
      )(implicit connection => stream(query),
        connection => a.delay(connection.close())
      )
    }

    def pipe[
      F[_],
      Query,
      Result
    ](implicit queryEnc: EncodeJson[Query],
      resultDec: DecodeJson[Result],
      pool: ConnectionPool,
      a: Async[F]
    ): Pipe[F, Query, Stream[F, Result]] =
      (queries: Stream[F, Query]) =>
        for {
          query <- queries
        } yield stream(query)

    trait syntax {
      implicit class SelectSyntax[Query](
        query: Query
      )(implicit queryEnc: EncodeJson[Query]
      ) {
        def iterator[
          Result
        ]()(implicit resultDec: DecodeJson[Result],
          connection: Connection
        ): CloseableIterator[Result] = {
          Select.iterator(query)
        }

        def stream[
          F[_],
          Result
        ]()(implicit resultDec: DecodeJson[Result],
          pool: ConnectionPool,
          a: Async[F]
        ): Stream[F, Result] = {
          Select.stream(query)
        }
      }
    }

    object syntax extends syntax

  }

  object Insert {
    def insert(
      value: Json
    )(implicit connection: Connection
    ): Unit = ???

    def insert[A](
      value: A
    )(implicit enc: EncodeJson[A],
      connection: Connection
    ): Unit =
      insert(enc(value))

    def sink[F[_], A](
      implicit a: Async[F],
      enc: EncodeJson[A],
      pool: ConnectionPool
    ): Sink[F, A] =
      (values: Stream[F, A]) =>
        for {
          value <- values
          _ <-
            Stream.bracket[F, Connection, Unit](
              a.delay(pool.get())
            )(implicit connection => Stream.eval(a.delay(insert(value))),
              connection => a.delay(connection.close())
            )
        } yield ()

    trait syntax {
      implicit class InsertSyntax[
        A
      ](value: A
      )(implicit enc: EncodeJson[A]
      ) {
        def insert()(implicit connection: Connection): Unit =
          Insert.insert(value)
      }
    }

    object syntax extends syntax
  }

  case class Select[
    Query,
    Result
  ](query: Query
  )(implicit queryEnc: EncodeJson[Query],
    resultDec: DecodeJson[Result]
  ) {
    def iterator()(implicit connection: Connection): CloseableIterator[Result] =
      Select.iterator(query)

    def stream[F[_]]()(implicit pool: ConnectionPool, a: Async[F]): Stream[F, Result] =
      Select.stream(query)
  }

  case class Insert[A](
    value: A
  )(implicit enc: EncodeJson[A]
  ) {
    def insert()(implicit connection: Connection): Unit =
      Insert.insert(value)
  }

  trait syntax
    extends Select.syntax
    with Insert.syntax

  object syntax extends syntax

}
