# SDBC by Rocketfuel

## Description
SDBC is a minimalist database API for Scala in the spirit of [Anorm](https://www.playframework.com/documentation/2.4.x/ScalaAnorm).
It currently supports [Apache Cassandra](http://cassandra.apache.org/), [H2](http://www.h2database.com/), [Microsoft SQL Server](http://www.microsoft.com/en-us/server-cloud/products/sql-server/), and [PostgreSQL](http://www.postgresql.org/).

JDBC connection pools are provided by [HikariCP](https://github.com/brettwooldridge/HikariCP). The pools can be created with a [HikariConfig](https://github.com/brettwooldridge/HikariCP) or [Typesafe Config](https://github.com/typesafehub/config) object.

There are additional packages that add support for [scalaz-stream](https://github.com/scalaz/scalaz-stream).

## Requirements

* Scala 2.11 or Scala 2.10
* Cassandra (2.11 only), H2, PostgreSQL, or Microsoft SQL Server

Include an implementation of the [SLF4J](http://slf4j.org/) logging interface, turn on debug logging, and all your query executions will be logged with the query text and the parameter name-value map.

## SBT Library Dependency

Packages exist on Maven Central for Scala 2.10 and 2.11. Cassandra packages only exist for Scala 2.11. The Scala 2.10 builds for PostgreSQL do not include support for arrays.

For Java 7, append "_java7" to the package name.

### Java 8

#### Cassandra

```scala
"com.rocketfuel.sdbc.cassandra" %% "datastax" % "1.0"
```

#### H2

```scala
"com.rocketfuel.sdbc.h2" %% "jdbc" % "1.0"
```

#### PostgreSql

```scala
"com.rocketfuel.sdbc.postgresql" %% "jdbc" % "1.0"
```

#### SQL Server

```scala
"com.rocketfuel.sdbc.sqlserver" %% "jdbc" % "1.0"
```

### Scalaz Streaming

#### Cassandra

```scala
"com.rocketfuel.sdbc.cassandra" %% "datastax-scalaz" % "1.0"
```

#### JDBC

```scala
"com.rocketfuel.sdbc.jdbc" %% "scalaz" % "1.0"
```

### Java 7

Append "-java7" to the package name.

## License

[BSD 3-Clause](http://opensource.org/licenses/BSD-3-Clause), so SDBC can be used anywhere Scala is used.

## Features

* Use generics to retrieve column values.
* Use generics to update column values.
* Use implicit conversions to convert rows into your own data types.
* Use Scala collection combinators to manipulate result sets.
* Use named parameters with queries.
* Query execution logging
* Use Java 8's java.time library, or Joda time for Java 7 and below.
* Scalaz streaming support by adding constructors to scalaz.stream.Process and scalaz.stream.io.

## Feature restrictions

### H2

* The H2 JDBC driver does not support getResultSet on inner arrays, so only 1 dimensional arrays are supported.
* The H2 JDBC driver does not support ResultSet#updateArray, so updating arrays is not supported.

### PostgreSQL

* Array support requires Scala 2.11 or newer.

## [Scaladoc](http://www.jeffshaw.me/sdbc/1.0)

## Java 8 time notes

| column type | column time zone | java.time type |
| --- | --- | --- |
| timestamp or datetime | GMT | Instant |
| timestamp or datetime | same as client | LocalDateTime |
| timestamp or datetime | not GMT and not client's | java.sql.Timestamp, then convert to LocalDateTime with server's time zone |
| timestamptz or timestamp with time zone or datetimeoffset |  | OffsetDateTime |
| date |  | LocalDate |
| time |  | LocalTime |
| timetz or time with time zone |  | OffsetTime |

## Joda time notes

| column type | column time zone | joda type |
| --- | --- | --- |
| timestamp or datetime | GMT | Instant |
| timestamp or datetime | same as client | LocalDateTime |
| timestamp or datetime | not GMT and not client's | java.sql.Timestamp, then convert to LocalDateTime with server's time zone |
| timestamptz or timestamp with time zone or datetimeoffset |  | DateTime |

## Examples

### Simple Query

```scala
import java.sql.DriverManager
import com.rocketfuel.sdbc.postgresql.jdbc._

//The JDBC connection can be implicitly converted into a Renorm connection.
val connection = DriverManager.getConnection("...")

val results =
	connection.iterator("SELECT * FROM tbl").map(
		(row: Row) =>
			row.get[Int]("key").get -> row.get[String]("value").get
	).toMap
```

### Use implicit conversion to map rows to other data types

```scala
import java.sql.DriverManager
import java.time.Instant
import com.rocketfuel.sdbc.postgresql.jdbc._

case class MyRow(
	id: Int,
	createdTime: Instant,
	message: Option[String]
)

implicit def RowToMyRow(row: Row): MyRow = {
    //Row#get[T] gives Option[T], so use Option#get on the result if the column is NOT NULL.
    val id = row.get[Int]("id").get
    val createdTime = row.get[Instant]("created_time").get
    val message = row.get[String]("message")

    MyRow(
        id,
        createdTime,
        message
    )
}

val query =
    Select[MyRow]("SELECT * FROM tbl WHERE message = @message").
    on("message" -> "hello there!")

val myRow = {
    /* If you are using a Select, SelectForUpdate, Update, or Batch value,
     * you don't need the connection until you call .iterator(),
     * .option() (jdbc only), iteratorAsync() (cassandra only), update(),
     * or batch().
     */
	implicit val connection: Connection = DriverManager.getConnection("...")
	try {
	    //Retrieve at most one row.
	    query.option()
    } finally {
        connection.close()
    }
}
```

### Set parameter values using string interpolation

Since StringContext doesn't allow the string value of the interpolated value to be
gotten (i.e. "$id" in the example below), such parameters are given consecutive integer names,
starting at 0.

```scala
implicit val connection: Connection = ???

val id = 1

select"SELECT * FROM table WHERE id = $id"
```

If you want to set the `id` value in the above query to a different value, you use the parameter "0".

```scala
select"SELECT * FROM table WHERE id = $id".on("0" -> 3)
```

You can use interpolated parameters and named parameters in the same query.

```scala
select"SELECT * FROM table WHERE id = $id AND something = @something".on("something" -> "hello")
```

### Reuse a query with different parameter values

The query classes Select, SelectForUpdate, Update, and Batch are immutable. Adding a parameter value returns a new object with that parameter set, leaving the old object untouched.

```scala
import java.sql.DriverManager
import java.time.Instant
import java.time.temporal.ChronoUnit
import com.rocketfuel.sdbc.postgresql.jdbc._

val query =
    Select[Int]("SELECT id FROM tbl WHERE message = @message AND created_time >= @time").
    on("message" -> "hello there!")

val minTimes = Seq(
    //today
    Instant.now().truncatedTo(ChronoUnit.DAYS),
    //this week
    Instant.now().truncatedTo(ChronoUnit.WEEKS)
)

val results = {
	implicit val connection: Connection = DriverManager.getConnection("...")
	val results =
        try {
            minTimes.map(minTime =>
                minTime -> query.on("time" -> minTime).iterator().toSeq
            )
        } finally {
            connection.close()
        }
    results.toMap
}
```

### Update
```scala
import java.sql.DriverManager
import com.rocketfuel.sdbc.postgresql.jdbc._

implicit val connection: Connection = DriverManager.getConnection("...")

val queryText = "UPDATE tbl SET unique_id = @uuid WHERE id = @id"

val parameters: ParameterList =
    Seq(
        "id" -> 3,
        "uuid" -> java.util.UUID.randomUUID()
    )

val updatedRowCount =
	connection.update(
	    queryText,
		parameters: _*
	)

//The above is equivalent to
val updatedRowCount2 =
    Update(queryText).on(parameters: _*).update()
```

### Batch Update
```scala
import java.sql.DriverManager
import com.rocketfuel.sdbc.postgresql.jdbc._

val batchUpdate =
	Batch("UPDATE tbl SET x = @x WHERE id = @id").
	addBatch("x" -> 3, "id" -> 10).
    addBatch("x" -> 4, "id" -> 11)

val updatedRowCount = {
    implicit val connection: Connection = DriverManager.getConnection("...")
    try {
    	batchUpdate.batch().sum
    finally {
        connection.close()
    }
}
```

### Update rows in a result set
```scala
import java.sql.DriverManager
import com.rocketfuel.sdbc.postgresql.jdbc._

implicit val connection: Connection = DriverManager.getConnection("...")

val actionLogger = Update("INSERT INTO action_log (account_id, action) VALUES (@account_id, @action)")

for (row <- SelectForUpdate("SELECT * FROM accounts").iterator()) {
	val accountId = row.get[Int]("account_id")
	if (accountId == Some(314)) {
		row.get("gold_nuggets").get = row.get[Int]("gold_nuggets").get + 159
		actionLogger.on("account_id" -> 314, "action" -> "added 159 gold nuggets").execute()
	}
}
```

### HikariCP connection pools with Typesafe Config support.

```scala
import com.typesafe.config.ConfigFactory
import com.rocketfuel.sdbc.postgresql.jdbc._

val pool = Pool(ConfigFactory.load())

val result = pool.withConnection { implicit connection =>
	f()
}
```

### Streaming parameter lists.

Constructors for Processes are added to the Process object via implicit conversion to JdbcProcess.

```scala
import com.rocketfuel.sdbc.h2.jdbc._
import scalaz.stream._
import com.rocketfuel.sdbc.scalaz.jdbc._

val parameterListStream: Process[Task, ParameterList] = ???

val update: Update = ???

implicit val pool: Pool = ???

parameterListStream.through(Process.jdbc.params.update(update)).run.run
```

### Streaming with type class support.

You can use one of the type classes for generating queries to create query streams from values.

For JDBC the type classes are Batchable, Executable, Selectable, Updatable. For Cassandra they are Executable and Selectable.

```scala
import com.rocketfuel.sdbc.h2.jdbc._
import scalaz.stream._
import com.rocketfuel.sdbc.scalaz.jdbc._

val pool: Pool = ???

implicit val SelectableIntKey = new Selectable[Int, String] {
  val selectString = Select[String]("SELECT s FROM tbl WHERE id = @id")

  override def select(id: Int): Select[String] = {
    selectString.on("id" -> id)
  }
}

val idStream: Process[Task, Int] = ???

//print the strings retreived from H2 using the stream of ids.
merge.mergeN(idStream.through(Process.jdbc.keys.select[Int, String](pool))).to(io.stdOutLines)
```

Suppose you use K keys to get values of some type T, and then use T to update rows.

```scala

val keyStream: Process[Task, K]

implicit val keySelectable = new Selectable[K, T] {...}

implicit val updatable = new Updatable[T] {...}

//Use the keys to select rows, and use the results to run an update.
keyStream.through(Process.jdbc.keys.select[K, T](pool)).to(Process.jdbc.update[T](pool)).run.run
```

## Changelog

### 1.0

* The sigil for a parameter is @ (was $).
* Added support for string interpolation for parameters.
* Cassandra support.
* Scalaz streaming helpers in com.rocketfuel.sdbc.scalaz.
* Connections and other types are no longer path dependent.
* Package paths are implementation dependent. (E.G. "import ...postgresql.jdbc" to use the JDBC driver to access PostgreSQL.)
* You can use [scodec](https://github.com/scodec/scodec)'s ByteVector instead of Array[Byte].
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
* Some method names were shortened: executeBatch() to batch(), executeUpdate() to update().
