# Adding a dialect

## Basic Components

The essence of SDBC is provided by several classes and type classes. They are:

### ParameterizedQuery

Your query class should extend ParameterizedQuery if it has named parameters but the query itself is not a String.

#### CompiledParameterizedQuery

Your query class should extend ParameterizedQuery if it has named parameters and the query itself is a String that must have positional parameters that are '?'. For instance, a CompiledParameterizedQuery created from "@x @x" will create the query "? ?" with the parameter positions Map("x" -> Set(0, 1)).

### Parameter

A type in the Parameter type class can be used to set parameter values. That is to say, any type `A` having a `Parameter[A]` in implicit scope can be used as an argument or part of an argument to the various `ParameterizedQuery#on` methods.

### RowConverter

RowConverters are functions which convert the raw result from the DBMS into a useful Scala type. Any type in the RowConverter type class can be selected, or any product or record made up of types in the Getter type class can be selected.

### Getter

Any type in the Getter type class can be used as part of a product or record

## Principles

SDBC is not necessarily purely functional. A particular dialect of SDBC can be more or less pure according to the tastes of the author. In the official dialects, only certain methods on queries or connection pools perform IO, but they are not typed specially to distinguish them from other methods.

A class should be created for each kind of query the DBMS supports. JDBC allows calling `.updateCount()` on a `ResultSet` that is a `SELECT` statement, which is absurd. Instead, create a query with methods that make sense for each query type.

SDBC queries should provide support for FS2 streams.

## Example

This example covers making an SDBC API for an imaginary DBMS, which stores JSON documents that are indexed by a Long, or you can query for JSON documents that intersect a given document. For instance,

```javascript
{"a":3}
```

would match

```javascript
{"message":"hi","a":3"}
```

but not

```javascript
{"message":"hi"}
```

The basic case allows only one or no documents to be returned, so one query method returning an Option is sufficient. Querying by intersection is more complex, as there could be zero or more results. Additionally, 

```scala
import com.rocketfuel.sdbc.base.IteratorUtils
import fs2.Stream
import fs2.util.Async
import org.json4s._

object Select {

  def get[A](key: Long)(implicit format: JsonFormat[A], connection: Connection): Option[A] = ???

  def get[
    A,
    B
  ](query: A
  )(implicit aFormat: JsonFormat[A],
    bFormat: JsonFormat[B],
    connection: Connection
  ): Option[A] = ???

  def iterator[
    Query,
    Result
  ](query: Query
  )(implicit aFormat: JsonFormat[Query],
    bFormat: JsonFormat[Result],
    connection: Connection
  ): CloseableIterator[Result] = ???
  
  def stream[
    F[_],
    Query,
    Result
  ](query: Query
  )(implicit aFormat: JsonFormat[Query],
    bFormat: JsonFormat[Result],
    connection: Connection,
    a: Async[F]
  ): Iterator[Result] = {
    IteratorUtils.fromCloseableIterator(a.delay(iterator(query)))
  }
  

}

case class Insert[A](key: Key, value: A)

```
