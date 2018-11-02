# SDBC by Rocketfuel

## Description
SDBC is a collection of database APIs for Scala. It originally was meant to be an alternative to [Anorm](https://www.playframework.com/documentation/2.5.x/Home), but has also borrowed ideas from from [Doobie](https://github.com/tpolecat/doobie). SDBC is not an ORM.

It currently supports [Apache Cassandra](http://cassandra.apache.org/), [H2](http://www.h2database.com/), [MariaDB](https://mariadb.org/), [Microsoft SQL Server](http://www.microsoft.com/en-us/server-cloud/products/sql-server/), and [PostgreSQL](http://www.postgresql.org/).

JDBC connection pools are provided by [HikariCP](https://github.com/brettwooldridge/HikariCP). The pools can be created with a [HikariConfig](https://github.com/brettwooldridge/HikariCP) or [Typesafe Config](https://github.com/typesafehub/config) object.

## [Examples](/EXAMPLES.md)

## [Adding a dialect](/DIALECT.md)

## Requirements

* Java 8
* Scala 2.11 or 2.12.
* Cassandra, H2, MariaDB, Microsoft SQL Server, or PostgreSQL

Include an implementation of the [SLF4J](http://slf4j.org/) logging interface, turn on debug logging, and all your query executions will be logged with the query text and the parameter name-value map.

## SBT Library Dependency

Packages exist on Maven Central for Scala 2.11 and 2.12.

#### Cassandra

```scala
"com.rocketfuel.sdbc" %% "cassandra-datastax" % "3.0.0"
```

#### H2

```scala
"com.rocketfuel.sdbc" %% "h2-jdbc" % "3.0.0"
```

#### MariaDB

```scala
"com.rocketfuel.sdbc" %% "mariadb-jdbc" % "3.0.0"
```

#### Microsoft SQL Server

```scala
"com.rocketfuel.sdbc" %% "sqlserver-jdbc" % "3.0.0"
```

#### PostgreSql

```scala
"com.rocketfuel.sdbc" %% "postgresql-jdbc" % "3.0.0"
```

## License

[BSD 3-Clause](http://opensource.org/licenses/BSD-3-Clause), so SDBC can be used anywhere Scala is used.

## Features

* Use generics and implicit conversions to retrieve column or row values.
* Get tuples or case classes from rows without any extra work. (thanks to [shapeless](https://github.com/milessabin/shapeless))
* Use tuples or case classes to set query parameters. (thanks to [shapeless](https://github.com/milessabin/shapeless))
* Use named parameters with queries.
* Use Scala collection combinators to manipulate result sets.
* Log queries with [SLF4J](http://www.slf4j.org/).
* Supports Java 8's java.time package.
* [FS2 Streams](https://github.com/functional-streams-for-scala/fs2) for streaming to or from a DBMS.
* Easily add column or row types.
* Type classes for each query type.

## Feature restrictions

### H2

* The H2 JDBC driver does not support getResultSet on inner arrays, so only 1 dimensional arrays are supported.
* The H2 JDBC driver does not support ResultSet#updateArray, so updating arrays is not supported.

## [Scaladoc](https://www.jeffshaw.me/sdbc/2.0/doc)

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

## Benchmarks

Starting with 2.0, there are benchmarks to ensure that some common operations don't have too much overhead over jdbc. There are two so far. The first batch inserts a collection of a case class. The second selects a colletion of a case class.

![select benchmark results](https://www.jeffshaw.me/sdbc/2.0/benchmarks/select.png)

![batch insert benchmark results](https://www.jeffshaw.me/sdbc/2.0/benchmarks/batch.png)

## Changelog

### 3.0.0
* Select, Selectable, Query, and Queryable now have map methods

### 2.0.2
* The wrong index was used for null parameters in jdbc queries.

### 2.0.1
* Correct publishing to Maven Central

### 2.0

* MariaDB & MySQL support
* JDBC Connection types are now per-DBMS (i.e. they are path dependent).
* Update scalaz streams to [FS2](https://github.com/functional-streams-for-scala/fs2).
* Stream support is built-in. Try using methods such as stream, pipe, and sink.
* Support for multiple result sets per query from MariaDB and SQL Server using MultiQuery.
* A datetime without an offset from a DBMS is interpreted as being in UTC rather than the system default time zone.
* No longer supports Scala 2.10.
* Moved database objects to `com.rocketfuel.sdbc.{Cassandra, H2, PostgreSql, SqlServer}`.
* Renamed Execute to Ignore.
* Added Insert, Delete, and their type classes.
* Typeclass methods are now in their respective companion objects. For example, `Selectable.select`.
* There are objects you can import to have type class methods added to type class members. For example `import com.rocketfuel.sdbc.PostgreSql.syntax._` would let you do something like `Person.Name(3, "Judy").update()` to update the name of record 3 to Judy.
* SelectForUpdate takes the update function as an argument. It returns a summary of the number of rows deleted, inserted, and updated.
* Maven package names changed again. Hopefully the last time.

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
