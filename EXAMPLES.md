#Examples

These can be pasted into a Scala REPL that has the H2, SDBC base, SDBC JDBC, and SDBC H2 jars in its class path.

## Query using a type classes

```scala
object TypeClassExample {
  import com.rocketfuel.sdbc.H2._
  import com.rocketfuel.sdbc.H2.syntax._

  case class Person(id: Int, name: String)

  object Person {
    case class Name(name: String)
    
    object Name {
      implicit val nameInsert: Insertable[Name] =
        Insert("INSERT INTO People (name) VALUES(@name)").insertable.product

       implicit val nameSelect: Selectable[Name, Person] =
        Select[Person]("SELECT * FROM People WHERE name = @name").selectable.product
    }
    
    object All {
      implicit val anySelect: Selectable[All.type, Person] =
        Select[Person]("SELECT * FROM People").selectable.constant
    }
  }
  
  val result = Connection.using("jdbc:h2:mem:example") {implicit connection =>
    Ignore.ignore("CREATE TABLE people (id int auto_increment PRIMARY KEY, name varchar(255))")
    
    Person.Name("Jeff").insert()
    Person.Name("Chrissy").insert()
    
    //yields (Vector(Person(1,Jeff), Person(2,Chrissy)),None)
    (Person.All.vector(), Person.Name("Joe").option())
  }

}

TypeClassExample.result
```

## Simple Query

```scala
object SimpleExample {
  import com.rocketfuel.sdbc.H2._

  case class Person(id: Int, name: String)

  object Person {
    case class Name(name: String)
  }

  val result = Connection.using("jdbc:h2:mem:example") {implicit connection =>
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
  }
}

SimpleExample.result
```

## Use implicit conversion to map rows to other data types

```scala
object ManualConversionExample {
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

ManualConversionExample.result
```

You will find that you can remove Log.valueOf, and the example will still work. As long as the columns in the result set have the same names as the fields in your case class, you do not need to manually create the converter.

## Set parameter values using string interpolation

Since StringContext doesn't allow the string value of the interpolated value to be
gotten (i.e. "$id" in the example below), such parameters are given consecutive integer names,
starting at 0.

```scala
object StringContextParametersExample {
  import com.rocketfuel.sdbc.H2._

  val id = 1

  val query = select"SELECT * FROM table WHERE id = $id"
}

//yields Map(0 -> ParameterValue(Some(1)))
StringContextParametersExample.query.parameters
```

If you want to set the `id` value in the above query to a different value, you use the parameter "0".

```scala
object OverrideStringContextParametersExample {
  import com.rocketfuel.sdbc.H2._

  val id = 1

  val query0 = select"SELECT * FROM table WHERE id = $id"

  val query1 = query0.on("0" -> 3)
}

//yields Map(0 -> ParameterValue(Some(1)))
OverrideStringContextParametersExample.query0.parameters

//yields Map(0 -> ParameterValue(Some(3)))
OverrideStringContextParametersExample.query1.parameters
```

You can use interpolated parameters and named parameters in the same query.

```scala
object InterpolatedAndNamedParametersExample {
  import com.rocketfuel.sdbc.H2._
  
  val id = 1
  val query = select"SELECT * FROM table WHERE id = $id AND something = @something".on("something" -> "hello")
}

//yields Map(0 -> ParameterValue(Some(1)), something -> ParameterValue(Some(hello)))
InterpolatedAndNamedParametersExample.query.parameters
```

## Reuse a query with different parameter values

The query classes, such as Select, are immutable. Adding a parameter value returns a new object with that parameter set, leaving the old object untouched.

```scala
object QueryReuseExample {
  import java.time.LocalDateTime
  import java.time.temporal.TemporalAdjusters
  import java.time.temporal.ChronoUnit
  import com.rocketfuel.sdbc.H2._

  val today = LocalDateTime.now().truncatedTo(ChronoUnit.DAYS)

  val minTimes = Seq(
    today,
    //a week ago
    today.minus(7, ChronoUnit.DAYS)
  )

  val message = "hello there!"
  val createdTime = today.minus(1, ChronoUnit.DAYS)

  val query =
    Select[Int]("SELECT id FROM tbl WHERE message = @message AND created_time >= @created_time").
      on("message" -> "hello there!")

  val results =
    Connection.using("jdbc:h2:mem:example;DB_CLOSE_DELAY=0") {implicit connection =>
      Ignore.ignore("CREATE TABLE tbl (id int auto_increment primary key, message varchar(255), created_time timestamp)")
      Ignore.ignore("INSERT INTO tbl (message, created_time) VALUES (@message, @createdTime)", Map("message" -> message, "createdTime" -> createdTime))
      val results =
        minTimes.map(minTime =>
          minTime -> query.on("created_time" -> minTime).vector()
        )
      results.toMap
    }
}

//yields Map(2017-01-01T00:00 -> Vector(), 2016-12-25T00:00 -> Vector(1))
QueryReuseExample.results
```

## Update
```scala
object UpdateExample {
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

UpdateExample.updatedRowCount
```

## Add a column type

### For Selecting

The following will not work, because the requested type is not supported natively by H2.
```scala
object NewColumnTypeExampleFailure {
  import com.rocketfuel.sdbc.H2._
  import scala.concurrent.duration._

  Select[Duration]("")
}
```

gives

```
error: Define an implicit function from Row to A, or create the missing Getters for parts of your product or record.
Error occurred in an application involving default arguments.
         Select[Duration]("")
                         ^
```

To resolve this, provide a Getter for Duration. SDBC will then be able to create a converter from a row to a Duration.

```scala
object NewColumnTypeExampleSuccess {
  import com.rocketfuel.sdbc.H2._
  import scala.concurrent.duration._

  implicit val DurationGetter: Getter[Duration] =
    Getter.converted[Duration, String](Duration(_))

  Select[Duration]("")
}
```

Providing a Getter also allows Duration to be selected as part of a product (case class or tuple) or shapeless record.

```scala
object NewColumnTypeToProductExample {
  import com.rocketfuel.sdbc.H2._
  import scala.concurrent.duration._

  implicit val DurationGetter: Getter[Duration] =
    Getter.converted[Duration, String](Duration(_))

  case class UserDuration(user: String, duration: Duration)

  Select[UserDuration]("")
}
```

### For parameters

The following will not work, because the parameter's type, Duration, is not supported natively by H2.

```scala
object NewParameterTypeExampleFailure {
  import com.rocketfuel.sdbc.H2._
  import scala.concurrent.duration._

  val duration = 5.seconds

  Update("@duration").on("duration" -> duration)
}
```

gives

```
error: type mismatch;
 found   : scala.concurrent.duration.FiniteDuration
 required: com.rocketfuel.sdbc.H2.ParameterValue
         Update("").on("duration" -> duration)
                                     ^
```

To resolve this, provide an implicit `Parameter[Duration]`. For example, maybe we want to insert durations as strings.

```scala
object NewParameterTypeExampleSuccess0 {
  import com.rocketfuel.sdbc.H2._
  import scala.concurrent.duration._

  implicit val durationParameter: Parameter[Duration] = {
    (value: Duration) => (statement: PreparedStatement, parameterIndex: Int) =>
      //Note the + 1, which converts SDBC's 0-based index to JDBC's 1-based index.
      statement.setString(parameterIndex + 1, value.toString)
      statement
  }

  val duration = 5.seconds

  val query = Update("@duration").on("duration" -> duration)
}

//yields Map(duration -> ParameterValue(Some(5 seconds)))
NewParameterTypeExampleSuccess0.query.parameters
```

Another possibility is that we want to store durations as milliseconds. This example uses the existing `Parameter[Long]` along with the Parameter.derived method.

```scala
object NewParameterTypeExampleSuccess1 {
  import com.rocketfuel.sdbc.H2._
  import scala.concurrent.duration._

  implicit val durationParameter: Parameter[Duration] = {
    implicit def durationToMillis(d: Duration): Long = d.toMillis
    Parameter.derived[Duration, Long]
  }

  val duration = 5.seconds

  val query = Update("@duration").on("duration" -> duration)
}

//yields Map(duration -> ParameterValue(Some(5 seconds))). The "5 seconds" will be converted to Long when the query parameters are set.
NewParameterTypeExampleSuccess1.query.parameters
```

Unfortunately, if you use an unsupported parameter in string interpolation, the error is not helpful.

```scala
object Example8Failure {
  import com.rocketfuel.sdbc.H2._
  import scala.concurrent.duration._

  val duration = 5.seconds

  update"$duration"
}
```

gives

```
error: could not find implicit value for parameter mapper: shapeless.ops.hlist.Mapper.Aux[com.rocketfuel.sdbc.H2.ToParameterValue.type,shapeless.::[scala.concurrent.duration.FiniteDuration,shapeless.HNil],MappedA]
```

## Update rows in a result set
```scala
object SelectForUpdateExample {
  import com.rocketfuel.sdbc.H2._

  def addGoldToRow(accountId: Int, amount: Int)(row: UpdatableRow): Unit = {
    if (row[Int]("id") == accountId) {
      row("gold_nuggets") = row[Int]("gold_nuggets") + amount
      row.updateRow()
    }
  }

  val accountUpdateQuery = SelectForUpdate("SELECT * FROM accounts")

  def addGold(accountId: Int, amount: Int)(implicit connection: Connection): UpdatableRow.Summary = {
    accountUpdateQuery.copy(rowUpdater = addGoldToRow(accountId, amount) _).on("id" -> accountId).update()
  }

  val selectedRowCount = Connection.using("jdbc:h2:mem:example;DB_CLOSE_DELAY=0") {implicit connection =>
    Ignore.ignore("CREATE TABLE accounts (id int auto_increment primary key, gold_nuggets int not null default (0))")
    Ignore.ignore("INSERT INTO accounts default values")

    addGold(1, 159)
  }
}

//yields Summary(0,0,1)
SelectForUpdateExample.selectedRowCount
```

## Streaming from JDBC

There are StreamUtils objects in each DBMS, which assist with creating safe FS2 Streams.

```scala
object StreamingH2Example {
  import com.rocketfuel.sdbc.H2._
  import com.zaxxer.hikari.HikariConfig
  import fs2.Stream
  import fs2.util.Async

  def ints[F[_]](config: HikariConfig)(implicit async: Async[F]): Stream[F, Int] =
    StreamUtils.pool(config) {implicit pool =>
      Select.stream[F, Int]("query")
    }
}
```

## Streaming from Cassandra

The following examples requires that the SDBC Cassandra driver is in your class path.

```scala
object StreamingCassandraExample0 {
  import com.datastax.driver.core.Cluster
  import com.rocketfuel.sdbc.Cassandra._
  import fs2.Stream
  import fs2.util.Async

  def ints[F[_]](initializer: Cluster.Initializer)(implicit async: Async[F]): Stream[F, Int] =
    StreamUtils.cluster(initializer) {implicit cluster =>
      StreamUtils.session {implicit session =>
        Query.stream[F, Int]("query")
      }
    }
}
```

## Streaming from multiple Cassandra keyspaces

Because a Cassandra Session is a connection pool, there are times we only want to keep on
Session open per keyspace. Cassandra's StreamUtils contains functions which let you query
from different Sessions while only ever opening one Session per keyspace. In other words,
the Session for each keyspace is memoized, and on stream completion they are all closed.

```scala
object StreamingCassandraExample1 {
  import com.datastax.driver.core.Cluster
  import com.rocketfuel.sdbc.Cassandra._
  import fs2.{Pipe, Stream}
  import fs2.util.Async

  def intsFromKeyspaces[F[_]](implicit cluster: Cluster, async: Async[F]): Pipe[F, String, Stream[F, Int]] = {
    keyspaces =>
      keyspaces.through(StreamUtils.keyspaces).map {implicit session =>
        Query.stream[F, Int]("...")
      }
  }

}
```

The remaining examples do not work in the Scala REPL.

## Batch Update
```scala
import com.rocketfuel.sdbc.H2._

val u = Update("UPDATE tbl SET x = @x WHERE id = @id")

val batchUpdate =
  Batch(
    u.on("x" -> 3, "id" -> 10),
    u.on("x" -> 4, "id" -> 11)
  )

val updatedRowCount = Connection.using("...") {implicit connection =>
  batchUpdate.batch().sum
}
```

## HikariCP connection pools with Typesafe Config support.

```scala
import com.typesafe.config.ConfigFactory
import com.rocketfuel.sdbc.PostgreSql._

val pool = Pool(ConfigFactory.load())

val result = pool.withConnection { implicit connection =>
  f()
}
```

## Streaming parameter lists.

```scala
import com.rocketfuel.sdbc.H2._
import fs._

val parameterListStream: Stream[Task, Parameters] = ???

val update = Update("...")

implicit val pool = Pool(...)

parameterListStream.through(update.pipe[Task].parameters).run.unsafeRun()
```

## Streaming Products

```scala
case class Id(id: Long)

case class Row(id: Long, value: String)

val parameters: Stream[Task, Id] = ???

val select = Select[Row]("SELECT id, value FROM table WHERE id = @id")

parameters.through(select.pipe[Task].products).runFree.run.unsafeRun()
```

## Streaming with type class support.

You can use one of the type classes for generating queries to create query streams from values.

For JDBC the type classes are Batchable, Ignorable, Selectable, SelectForUpdatable, and Updatable. For Cassandra there is Queryable.

```scala
import com.rocketfuel.sdbc.H2._
import fs2._
import fs2.concurrent.join

implicit val pool = Pool(...)

case class Id(id: Int)

implicit val SelectableIntKey: Selectable[Id, String] =
  Select[String]("SELECT value FROM table WHERE id = @id").
    selectable[Id].product

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
