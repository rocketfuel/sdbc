# Adding a dialect

## Principles

The original goal of SDBC was to create a Scala-native interface for JDBC. Now the focus is on being able to perform query operations on any value that you would like to use as the basis of a query. For JDBC this is usually a `String` with some parameters. Or maybe you have a class that represents a row or lookup key. You should be able to query from those as well.

SDBC is not purely functional.

SDBC is not a fully-featured ORM, but provides facilities for mapping case classes to query parameters and for extracting case classes from result sets.

A class should be created for each kind of query the DBMS supports. JDBC allows calling `.updateCount()` on a `ResultSet` that is a `SELECT` statement, which is absurd. Instead, create classes for each query type.

For each query class, there should be a type class. If appropriate, query classes should provide factory methods for type classes. There should be optional syntax on types which are members of query type classes. For instance, if there is a `Selectable[Int, Value]` in scope, then `3.option[Value]()` can get the `Value` whose primary key is `3`.

SDBC queries should provide support for [FS2](https://github.com/functional-streams-for-scala/fs2) streams, pipes, and sinks.

An SDBC-like API does not necessarily have to rely on an SDBC library, but the base and jdbc packages provide utilities that can make the task easier.

## Example

This example covers making an SDBC API for a simple DBMS, which stores JSON documents. You can query for JSON documents that intersect a given document. For instance,

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

The code for the example is out of order and incomplete for demonstration purposes. For the full runnable example, see [jsondbms](/examples/src/main/scala/com/example/jsondbms/).

This implementation does not provide classes for Select or Insert. The equivalent functionality is already provided by [Argonaut](http://argonaut.io/), a JSON library for Scala.

### Imports

```scala
  import argonaut.JsonIdentity._
  import argonaut.JsonScalaz._
  import argonaut._
  import fs2.{Pipe, Stream}
  import scala.collection.JavaConverters._
  import scalaz.syntax.equal._
```

### Select

```scala
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
  } yield result.as[Result].value.ge
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
  Stream.eval(a.delay(iterator(query))).flatMap(Stream.fromIterator(_))

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
```

### Insert

```scala
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
): Pipe[F, A, Unit] =
  (values: Stream[F, A]) =>
    for {
      value <- values
      _ <- Stream.eval(a.delay(insert(value)))
    } yield ()
```

### Type Classes

We don't have to provide the type classes, since Argonaut provides them. Anything that is insertable or can be a select key is an `EncodeJson`, and any select result is a `DecodeJson`.

### Syntax

An SDBC API should provide convenience methods for values in query type classes. You could also add the ability to run query operations directly on `Json`s.

```scala
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

implicit class InsertSyntax[
  A
](value: A
)(implicit enc: EncodeJson[A]
) {
  def insert()(implicit connection: JsonDb): Unit =
    Insert.insert(value)
}
```

### Example Use

We can use the API to manage a database of pirates.

```scala
case class Pirate(
  ship: String,
  name: String,
  shoulderPet: String //every pirate needs some animal on his or her shoulder
)

object Pirate {
  implicit val PirateCodecJson: CodecJson[Pirate] =
    casecodec3(Pirate.apply, Pirate.unapply)("ship", "name", "shoulderPet")
}

case class Ship(
  ship: String
)

object Ship {
  implicit val ShipCodecJson: CodecJson[Ship] =
    casecodec1(Ship.apply, Ship.unapply)("ship")
}

//Thanks http://www.seventhsanctum.com/generate.php?Genname=pirateshipnamer
val hadesPearl = Ship("Hades' Pearl")
val oceansEvilPoison = Ship("Ocean's Evil Poison")

//Thanks to http://gangstaname.com/names/pirate
val pirates =
  Seq(
    Pirate(hadesPearl.ship, "Fartin' Garrick Hellion", "Cap'n Laura Cannonballs"),
    Pirate(hadesPearl.ship, "Pirate Ann Marie the Well-Tanned", "Cheatin' Louise Bonny"),
    Pirate(oceansEvilPoison.ship, "Fish Breath Rupert", "Rancid Dick Scabb")
  )
```

We can insert them.

```scala
Stream[IO, Pirate](pirates.toSeq: _*).through(Insert.sink)
```

Then, we can query for the crew members of Hades' Pearl, and print them to the stdout.

```scala
Stream.resource(Blocker[IO]).flatMap { blocker =>
  hadesPearl.stream[IO, Pirate].through(printPirates).through(fs2.io.stdout[IO](blocker))
}
```

The output from running Pirate.scala is
```
Pirate(Hades' Pearl,Fartin' Garrick Hellion,Cap'n Laura Cannonballs)
Pirate(Hades' Pearl,Pirate Ann Marie the Well-Tanned,Cheatin' Louise Bonny)
```
