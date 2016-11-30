# SDBC by Rocketfuel

## Description
SDBC is a minimalist database API for Scala in the spirit of [Anorm](https://www.playframework.com/documentation/2.4.x/ScalaAnorm).
It currently supports [Apache Cassandra](http://cassandra.apache.org/), [H2](http://www.h2database.com/), [Microsoft SQL Server](http://www.microsoft.com/en-us/server-cloud/products/sql-server/), and [PostgreSQL](http://www.postgresql.org/).

JDBC connection pools are provided by [HikariCP](https://github.com/brettwooldridge/HikariCP). The pools can be created with a [HikariConfig](https://github.com/brettwooldridge/HikariCP) or [Typesafe Config](https://github.com/typesafehub/config) object.

There are additional packages that add support for [scalaz-stream](https://github.com/scalaz/scalaz-stream).

## Requirements

* Java 8
* Scala 2.11 or 2.12.
* Cassandra, H2, Microsoft SQL Server, or PostgreSQL

Include an implementation of the [SLF4J](http://slf4j.org/) logging interface, turn on debug logging, and all your query executions will be logged with the query text and the parameter name-value map.

## SBT Library Dependency

Packages exist on Maven Central for Scala 2.11 and 2.12.

#### Cassandra

```scala
"com.rocketfuel.sdbc.cassandra" %% "datastax" % "2.0"
```

#### H2

```scala
"com.rocketfuel.sdbc.h2" %% "jdbc" % "2.0"
```

#### PostgreSql

```scala
"com.rocketfuel.sdbc.postgresql" %% "jdbc" % "2.0"
```

#### SQL Server

```scala
"com.rocketfuel.sdbc.sqlserver" %% "jdbc" % "2.0"
```

## License

[BSD 3-Clause](http://opensource.org/licenses/BSD-3-Clause), so SDBC can be used anywhere Scala is used.

## Features

* Use generics and implicit conversions to retrieve column or row values.
* Get tuples or case classes from rows without any extra work. (thanks to [shapeless](https://github.com/milessabin/shapeless))
* Use tuples or case classes to set query parameters. (thanks to [shapeless](https://github.com/milessabin/shapeless))
* Use named parameters with queries.
* Use Scala collection combinators to manipulate result sets.
* Query execution logging.
* Supports Java 8's java.time package.
* [FS2 Streams](https://github.com/functional-streams-for-scala/fs2) for streaming to or from a DBMS.
* Easily add column or row types.
* Type classes for each query type.

## Feature restrictions

### H2

* The H2 JDBC driver does not support getResultSet on inner arrays, so only 1 dimensional arrays are supported.
* The H2 JDBC driver does not support ResultSet#updateArray, so updating arrays is not supported.

## [Scaladoc](http://www.jeffshaw.me/sdbc/2.0)

## Java 8 time notes

| column type | column time zone | java.time type |
| --- | --- | --- |
| timestamp or datetime | GMT | Instant |
| timestamp or datetime | same as client | LocalDateTime |
| timestamp or datetime | not GMT and not client's | Timestamp, then convert to LocalDateTime with desired time zone |
| timestamptz or timestamp with time zone or datetimeoffset |  | OffsetDateTime |
| date |  | LocalDate |
| time |  | LocalTime |
| timetz or time with time zone |  | OffsetTime |

## Examples

These can be pasted into a Scala REPL that has the H2, SDBC base, SDBC JDBC, and SDBC H2 jars in its class path.

### Simple Query

```scala
object Example0 {
  import java.sql.DriverManager
  import com.rocketfuel.sdbc.H2._

  case class Person(id: Int, name: String)

  object Person {
    case class Name(name: String)
  }

  val result = Connection.using("jdbc:h2:mem:example") {implicit connection =>
    try {
      Ignore.ignore("CREATE TABLE people (id int auto_increment PRIMARY KEY, name varchar(255))")

      //Use named parameters and a case class to insert a row.
      Ignore.ignore("INSERT INTO people (name) VALUES (@name)", Parameters.product(Person.Name("jeff")))

      //prints "Person(1, jeff)"
      for (x <- Select.iterator[Person]("SELECT * FROM people"))
        println(x)

      /*
      You can select directly to tuples if you name your columns appropriately.
      If you select Tuple2s, you can create a map from the results.
      */

      //yields Map(1 -> "jeff")
      Select.iterator[(Int, String)]("SELECT id AS _1, name AS _2 FROM people").toMap
    } finally connection.close()
  }
}

Example0.result
```

### Use implicit conversion to map rows to other data types

```scala
object Example1 {
  import java.time.Instant
  import com.rocketfuel.sdbc.H2._

  case class Log(
    id: Int,
    createdTime: Instant,
    message: Option[String]
  )

  object Log {
    implicit def valueOf(row: ConnectedRow): Log = {
      val id = row[Int]("id")
      val createdTime = row[Instant]("createdTime")
      val message = row[Option[String]]("message")

      Log(
        id = id,
        createdTime = createdTime,
        message = message
      )
    }

    val create =
      Ignore(
        """CREATE TABLE log (
        |  id int auto_increment,
        |  createdTime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        |  message varchar(max)
        |)""".stripMargin
      )

    val drop = Ignore("DROP TABLE log")

    case class Message(
      message: String
    )

    val insert =
      Update("INSERT INTO log (message) VALUES (@message)")

    val selectByMessage =
      Select[Log]("SELECT * FROM log WHERE message = @message")
  }


  val result =
    Connection.using("jdbc:h2:mem:example;DB_CLOSE_DELAY=0") {implicit connection =>
      Log.create.ignore()

      Log.insert.onProduct(Log.Message("hi")).update()

      //yields Some(Log(1,2016-10-17T19:41:43.164Z,Some(hi)))
      Log.selectByMessage.onProduct(Log.Message("hi")).option()
    }
}

Example1.result
```

### Set parameter values using string interpolation

Since StringContext doesn't allow the string value of the interpolated value to be
gotten (i.e. "$id" in the example below), such parameters are given consecutive integer names,
starting at 0.

```scala
val example2 = {
  import com.rocketfuel.sdbc.H2._

  val id = 1

  val query = select"SELECT * FROM table WHERE id = $id"

  //yields Map(0 -> ParameterValue(Some(1)))
  query.parameters
}
```

If you want to set the `id` value in the above query to a different value, you use the parameter "0".

```scala
val example3 = {
  import com.rocketfuel.sdbc.H2._

  val id = 1

  val query = select"SELECT * FROM table WHERE id = $id"

  //yields Map(0 -> ParameterValue(Some(1)))
  query.on("0" -> 3).parameters
}
```

You can use interpolated parameters and named parameters in the same query.

```scala
val example4 = {
  import com.rocketfuel.sdbc.H2._
  select"SELECT * FROM table WHERE id = $id AND something = @something".on("something" -> "hello")
}
```

### Reuse a query with different parameter values

The query classes Select, SelectForUpdate, Update, and Batch are immutable. Adding a parameter value returns a new object with that parameter set, leaving the old object untouched.

```scala
object Example5 {
  import java.sql.DriverManager
  import java.time.LocalDateTime
  import java.time.temporal.TemporalAdjusters
  import java.time.temporal.ChronoUnit
  import com.rocketfuel.sdbc.H2._

  val query =
    Select[Int]("SELECT id FROM tbl WHERE message = @message AND created_time >= @created_time").
      on("message" -> "hello there!")

  val today = LocalDateTime.now().truncatedTo(ChronoUnit.DAYS)

  val minTimes = Seq(
    today,
    //this week
    today.minus(today.getDayOfWeek().getValue, ChronoUnit.DAYS)
  )

  val results =
    Connection.using("jdbc:h2:mem:example;DB_CLOSE_DELAY=0") {implicit connection =>
      Ignore.ignore("CREATE TABLE tbl (id int auto_increment primary key, message varchar(255), created_time timestamp)")
      val results =
        minTimes.map(minTime =>
          minTime -> query.on("created_time" -> minTime).vector()
        )
      results.toMap
    }
}

Example5.results
```

### Update
```scala
object Example6 {
  import java.sql.DriverManager
  import com.rocketfuel.sdbc.H2._

  val query = Update("UPDATE tbl SET unique_id = @uuid WHERE id = @id")

  val parameters: Parameters =
    Map(
      "id" -> 3,
      "uuid" -> java.util.UUID.randomUUID()
    )

  val updatedRowCount = Connection.using("jdbc:h2:mem:example;DB_CLOSE_DELAY=0") {implicit connection =>
    Ignore.ignore("CREATE TABLE tbl (id int auto_increment primary key, unique_id uuid default(random_uuid()))")
    for (_ <- 0 until 3)
      Update.update("INSERT INTO tbl default values")

  	query.onParameters(parameters).update()
  }
}

Example6.updatedRowCount
```

### Add a column type

The following will not work, because the requested type is not supported natively by SDBC.
```scala
object Example7Failure {
  import java.sql.DriverManager
  import com.rocketfuel.sdbc.H2._
  import scala.concurrent.duration._

  Select[Duration]("")
}
```

gives

```
error: Define an implicit function from ConnectedRow to A, or create the missing Getters for parts of your product or record.
Error occurred in an application involving default arguments.
         Select[Duration]("SELECT duration from table")
```

To resolve this, provide a Getter for Duration. SDBC will then be able to create a converter from a row to a Duration.

```scala
object Example7Success {
  import java.sql.DriverManager
  import com.rocketfuel.sdbc.H2._
  import scala.concurrent.duration._

  implicit val DurationGetter: Getter[Duration] =
    Getter.ofParser[Duration](Duration(_))

  Select[Duration]("")
}
```

Providing a Getter also allows Duration to be selected as part of a product (case class or tuple) or shapeless record.

```scala
object Example7Product {
  import java.sql.DriverManager
  import com.rocketfuel.sdbc.H2._
  import scala.concurrent.duration._

  implicit val DurationGetter: Getter[Duration] =
    Getter.ofParser[Duration](Duration(_))

  case class UserDuration(user: String, duration: Duration)

  Select[UserDuration]("")
}
```

The remaining examples do not work in the Scala REPL.

### Batch Update
```scala
import java.sql.DriverManager
import com.rocketfuel.sdbc.H2._

val batchUpdate =
  Batch("UPDATE tbl SET x = @x WHERE id = @id").
    add("x" -> 3, "id" -> 10).
    add("x" -> 4, "id" -> 11)

val updatedRowCount = Connection.using("...") {implicit connection =>
  batchUpdate.batch().sum
}
```

### Update rows in a result set
```scala
val actionLogger = Update("INSERT INTO action_log (account_id, action) VALUES (@accountId, @action)")

val accountUpdateQuery = SelectForUpdate("SELECT * FROM accounts WHERE id = @id")

case class Action(accountId: Int, action: String)

def addGold(accountId: Int, quantity: Int)(implicit connection: Connection): Unit = {
  val iterator = accountUpdateQuery.on("id" -> accountId).iterator()
  try for (row <- iterator) {
    row("gold_nuggets") = row[Int]("gold_nuggets") + 159
    actionLogger.onProduct(Action(accountId, s"added $quantity gold nuggets")).update()
  } finally iterator.close()
}
```

### HikariCP connection pools with Typesafe Config support.

```scala
import com.typesafe.config.ConfigFactory
import com.rocketfuel.sdbc.PostgreSql._

val pool = Pool(ConfigFactory.load())

val result = pool.withConnection { implicit connection =>
  f()
}
```

### Streaming parameter lists.

```scala
import com.rocketfuel.sdbc.H2._
import fs._

val parameterListStream: Stream[Task, Parameters] = ???

val update = Update("...")

implicit val pool = Pool(...)

parameterListStream.through(update.pipe[Task].parameters).run.unsafeRun()
```

### Streaming Products

```scala
case class Id(id: Long)

case class Row(id: Long, value: String)

val parameters: Stream[Task, Id] = ???

val select = Select[Row]("SELECT id, value FROM table WHERE id = @id")

parameters.through(select.pipe[Task].products).runFree.run.unsafeRun()
```

### Streaming with type class support.

You can use one of the type classes for generating queries to create query streams from values.

For JDBC the type classes are Batchable, Executable, Selectable, SelectForUpdatable, and Updatable. For Cassandra there is Queryable.

```scala
import com.rocketfuel.sdbc.H2._
import fs2._
import fs2.concurrent.join

implicit val pool = Pool(...)

case class Id(id: Int)

implicit val SelectableIntKey: Selectable[Id, String] = {
  val query = Select[String]("SELECT value FROM table WHERE id = @id")
  Selectable[Id, String](id => query.on("id" -> id))
}

val idStream: Stream[Task, Int] = ???

//print the strings retrieved from H2 using the stream of ids.
join(10)(idStream.through(Selectable.pipe[Task, Id, String].products)).to(io.stdOutLines).run.unsafeRun()
```

Suppose you use K keys to get values of some type T, and then use T to update rows.

```scala
val keyStream: Stream[Task, K] = ???

implicit val keySelectable = Selectable[K, T]((key: K) => ???)

implicit val updatable = Updatable[T]((key: T) => ???)

//Use the keys to select rows, and use the each result to run an update.
keyStream.through(Selectable.pipe[Task, K, T].products).map(Updatable.update).run.unsafeRun()
```

## Benchmarks

Starting with 2.0, there are benchmarks to ensure that some common operations don't have too much overhead over jdbc. There are two so far. The first batch inserts a collection of a case class. The second selects a colletion of a case class.

![select benchmark results](https://www.jeffshaw.me/sdbc/2.0/benchmarks/select.png)

![batch insert benchmark results](https://www.jeffshaw.me/sdbc/2.0/benchmarks/batch.png)

## Changelog

### 2.0

* JDBC Connection types are now per-DBMS (i.e. they are path dependent).
* Update scalaz streams to [FS2](https://github.com/functional-streams-for-scala/fs2).
* Stream support is built-in. Try using methods such as stream, pipe, and sink.
* Support for multiple result sets per query from SQL Server using MultiQuery.
* A datetime without an offset from a DBMS is interpreted as being in UTC rather than the system default time zone.
* No longer supports Scala 2.10.
* Moved database objects to `com.rocketfuel.sdbc.{Cassandra, H2, PostgreSql, SqlServer}`.
* Renamed Execute to Ignore.
* Typeclass methods are now in their respective companion objects. For example, `Selectable.select`.

### 1.0

* The sigil for a parameter is `'@'` (was `'$'`).
* Added support for string interpolation for parameters.
* Cassandra support.
* Scalaz streaming helpers in com.rocketfuel.sdbc.scalaz.
* Connections and other types are no longer path dependent.
* Package paths are implementation dependent. (E.G. `import ...postgresql.jdbc` to use the JDBC driver to access PostgreSQL.)
* You can use [scodec](https://github.com/scodec/scodec)'s `ByteVector` instead of `Array[Byte]`.
* Remove Byte getters, setters, and updaters from PostgreSQL (the JDBC driver uses Short under the hood).

### 0.10

#### Java 7

* Added Joda Time support.

### 0.9

* Only Hikari configuration parameters are sent to the HikariConfig constructor.
* Added support for Java 7.

### 0.8

* XML getters and setters use Node instead of Elem.

### 0.7

* PostgreSQL was missing some keywords in its Identifier implementation.

### 0.6

* Add support for H2.
* Test packages have better support for building and destroying test catalogs.
* Some method names were shortened: `executeBatch()` to `batch()`, `executeUpdate()` to `update()`.
