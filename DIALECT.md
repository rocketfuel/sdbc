# Adding a dialect

## Principles

The original goal of SDBC was to create a clean abstraction layer over JDBC for Scala. Now the focus is on being able to perform query operations on any value that you would like to use as the basis of a query. For JDBC this is usually a `String` with some parameters. Or maybe you have a case class that represents a row or lookup key. You should be able to query from those as well.

SDBC is not necessarily purely functional. A particular dialect of SDBC can be more or less pure according to the tastes of the author. In the official dialects, only certain methods on queries or connection pools perform IO, but they are not typed specially to distinguish them from other methods.

SDBC is not an ORM.

A class should be created for each kind of query the DBMS supports. JDBC allows calling `.updateCount()` on a `ResultSet` that is a `SELECT` statement, which is absurd. Instead, create a query with methods that make sense for each query type.

For each query class, there should be a type class. The query classes should provide factory methods for type classes. There should be optional syntax on types which are members of query type classes. For instance, if there is a `Selectable[Int, Value]` in scope, then `3.option[Value]()` can get the `Value` whose primary key is `3`.

SDBC queries should provide support for FS2 streams, pipes, and sinks.

A SDBC-like API does not necessarily have to rely on a SDBC library, but the base and jdbc packages provide utilities that can make the task easier.

## Example

This example covers making an SDBC API for an imaginary DBMS, which stores JSON documents. You can query for JSON documents that intersect a given document. For instance,

```javascript
{"a":3}
```

would match

```javascript
{"message":"hi","a":3}
```

but not

```javascript
{"message":"hi"}
```

```scala
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
}
```

With the basic functionality in place, we can add type classes.

```scala

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

trait Selectable[Query, Result] {
  def select(
    query: Query
  ): Select[Query, Result]
}

object Selectable {

  implicit def apply[
    Query,
    Result
  ](implicit queryEnc: EncodeJson[Query],
    resultDec: DecodeJson[Result]
  ) =
    new Selectable[Query, Result] {
      override def select(query: Query): Select[Query, Result] =
        Select[Query, Result](query)
    }

}

case class Insert[A](
  value: A
)(implicit enc: EncodeJson[A]
) {
  def insert()(implicit connection: Connection): Unit =
    Insert.insert(value)
}

trait Insertable[Key, A] {
  def insert(key: Key)(implicit enc: EncodeJson[A]): Insert[A]
}
```

We can use the API to manage a database of pirates.

```scala
case class Pirate(
  ship: String,
  name: String,
  battleCry: String,
  shoulderPet: String //A pirate's pet always sits on its owner's shoulder.
)

object Pirate {
  implicit val PirateCodecJson: CodecJson[Pirate] =
    casecodec4(Pirate.apply, Pirate.unapply)("ship", "name", "battleCry", "shoulderPet")
}

//Thanks to http://gangstaname.com/names/pirate and http://www.seventhsanctum.com/generate.php?Genname=pirateshipnamer
val pirates =
  Set(
    Pirate("Killer's Fall", "Fartin' Garrick Hellion", "yar", "Cap'n Laura Cannonballs"),
    Pirate("Pirate's Shameful Poison", "Pirate Ann Marie the Well-Tanned", "arrr", "Cheatin' Louise Bonny")
  )
```

We can insert them.

```scala
Stream(pirates.toSeq: _*).covary[Task].to(Insert.sink).run.unsafeRun()
```

Then, we can query for the crew members of Killer's Fall, and print them to the stdout.

```scala
case class Ship(
  ship: String
)

object Ship {
  implicit val ShipCodecJson: CodecJson[Ship] =
    casecodec1(Ship.apply, Ship.unapply)("ship")
}

val killersFallCrewMembers: Stream[Task, Pirate] =
  Select.stream[Task, Ship, Pirate](Ship("Killer's Fall"))

killersFallCrewMembers.through(pirateLines).to(fs2.io.stdout).run.unsafeRun()
```

An SDBC API should provide convenience methods for values in query type classes. For this example DBMS, the type class for a query is `EncodeJson`, and the type class for a result is `DecodeJson`.

```scala
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

implicit class InsertSyntax[
  A
](value: A
)(implicit enc: EncodeJson[A]
) {
  def insert()(implicit connection: Connection): Unit =
    Insert.insert(value)
}
```

This lets us do things like,

```scala
for (pirate <- pirates)
  pirate.insert()

killersFall.iterator[Pirate]()
```
