package com.example

package object jsondbms {

  import argonaut.JsonIdentity._
  import argonaut.JsonScalaz._
  import argonaut._
  import com.rocketfuel.sdbc.base.IteratorUtils
  import fs2.util.Async
  import fs2.{Pipe, Sink, Stream}
  import scala.collection.JavaConverters._
  import scalaz.syntax.equal._

  class JsonDb {
    pool =>

    private val db =
      new java.util.concurrent.ConcurrentLinkedQueue[Json]()

    def insert(value: Json): Unit = {
      db.add(value)
    }

    def select(query: Json): Iterator[Json] = {
      for {
        value <- db.iterator().asScala
        merged = value.deepmerge(query)
        if merged === value
      } yield value
    }
  }

  object Select {

    def iterator[
      Query,
      Result
    ](query: Query
    )(implicit queryEnc: EncodeJson[Query],
      resultDec: DecodeJson[Result],
      connection: JsonDb
    ): Iterator[Result] =
      for {
        result <- connection.select(query.asJson)
      } yield result.as[Result].value.get

    def stream[
      F[_],
      Query,
      Result
    ](query: Query
    )(implicit queryEnc: EncodeJson[Query],
      resultDec: DecodeJson[Result],
      pool: JsonDb,
      a: Async[F]
    ): Stream[F, Result] = {
      IteratorUtils.toStream(a.delay(iterator(query)))
    }

    def pipe[
      F[_],
      Query,
      Result
    ](implicit queryEnc: EncodeJson[Query],
      resultDec: DecodeJson[Result],
      pool: JsonDb,
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
          connection: JsonDb
        ): Iterator[Result] = {
          Select.iterator(query)
        }

        def stream[
          F[_],
          Result
        ]()(implicit resultDec: DecodeJson[Result],
          pool: JsonDb,
          a: Async[F]
        ): Stream[F, Result] = {
          Select.stream(query)
        }
      }
    }

    object syntax extends syntax

  }

  object Insert {
    def insert[A](
      value: A
    )(implicit enc: EncodeJson[A],
      connection: JsonDb
    ): Unit =
      connection.insert(value.asJson)

    def sink[F[_], A](
      implicit a: Async[F],
      enc: EncodeJson[A],
      pool: JsonDb
    ): Sink[F, A] =
      (values: Stream[F, A]) =>
        for {
          value <- values
          _ <- Stream.eval(a.delay(insert(value)))
        } yield ()

    trait syntax {
      implicit class InsertSyntax[
        A
      ](value: A
      )(implicit enc: EncodeJson[A]
      ) {
        def insert()(implicit connection: JsonDb): Unit =
          Insert.insert(value)
      }
    }

    object syntax extends syntax
  }

  trait syntax
    extends Select.syntax
    with Insert.syntax

  object syntax extends syntax

}
