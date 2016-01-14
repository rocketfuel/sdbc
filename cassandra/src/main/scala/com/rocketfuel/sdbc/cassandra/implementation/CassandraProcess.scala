package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core
import com.google.common.util.concurrent.{Futures, FutureCallback, ListenableFuture}
import scala.collection.concurrent.TrieMap
import scala.collection.convert.wrapAsScala._
import scalaz.{DLeft, DRight}
import shapeless.ops.hlist._
import shapeless.ops.record.{MapValues, Keys}
import shapeless.{LabelledGeneric, HList}
import scalaz.concurrent.Task
import scalaz.stream._

trait CassandraProcess {
  self: Cassandra =>

  object HasCassandraProcess {
    val cassandra = CassandraProcess
  }

  implicit def ProcessToCassandraProcess(x: Process.type): HasCassandraProcess.type = {
    HasCassandraProcess
  }

  trait CassandraProcess {

    /**
      * Create a stream from one query, whose result is ignored.
      * @param execute
      * @param session
      * @return a stream of one () value.
      */
    def execute(execute: Execute)(implicit session: Session): Process[Task, Unit] = {
      Process.eval(ProcessUtils.runExecute(execute))
    }

    /**
      * Create a stream of values from a query's results.
      * @param select
      * @param session
      * @tparam T
      * @return a stream of the query results.
      */
    def select[T](select: Select[T])(implicit session: Session): Process[Task, T] = {
      ProcessUtils.runSelect(select)
    }

    object product {
      def execute[
        A,
        Repr <: HList,
        MappedRepr <: HList,
        ReprKeys <: HList
      ](execute: Execute
      )(implicit session: Session,
        genericA: LabelledGeneric.Aux[A, Repr],
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Sink[Task, A] = {
        sink.lift[Task, A] {param =>
          ProcessUtils.runExecute(execute.onProduct(param))
        }
      }

      def select[
        A,
        Value,
        Repr <: HList,
        MappedRepr <: HList,
        ReprKeys <: HList
      ](select: Select[Value]
      )(implicit session: Session,
        genericA: LabelledGeneric.Aux[A, Repr],
        mapper: Mapper.Aux[ToParameterValue.type, Repr, MappedRepr],
        keys: Keys.Aux[Repr, ReprKeys],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Channel[Task, A, Process[Task, Value]] = {
        channel.lift[Task, A, Process[Task, Value]] { param =>
          Task.delay(ProcessUtils.runSelect[Value](select.onProduct(params)))
        }
      }

      def executeWithKeyspace[
        A,
        Repr <: HList,
        MappedRepr <: HList,
        ReprKeys <: HList
      ](execute: Execute
      )(implicit genericA: LabelledGeneric.Aux[A, Repr],
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Cluster => Sink[Task, (String, A)] = {
        ProcessUtils.forClusterWithKeyspaceAux[A, Unit] { param => implicit session =>
          ProcessUtils.runExecute(execute.onProduct(param))
        }
      }

      def selectWithKeyspace[
        A,
        Value,
        Repr <: HList,
        MappedRepr <: HList,
        ReprKeys <: HList
      ](select: Select[Value]
      )(implicit genericA: LabelledGeneric.Aux[A, Repr],
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Cluster => Channel[Task, (String, A), Process[Task, Value]] = {
        ProcessUtils.forClusterWithKeyspaceAux[A, Process[Task, Value]] { param => implicit session =>
          Task.delay(ProcessUtils.runSelect[Value](select.onProduct(param)))
        }
      }
    }

    object record {
      def execute[
        Repr <: HList,
        MappedRepr <: HList,
        ReprKeys <: HList
      ](execute: Execute
      )(implicit session: Session,
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Sink[Task, Repr] = {
        sink.lift[Task, Repr] {param =>
          ProcessUtils.runExecute(execute.onRecord(param))
        }
      }

      def select[
        Value,
        Repr <: HList,
        MappedRepr <: HList,
        ReprKeys <: HList
      ](select: Select[Value]
      )(implicit session: Session,
        keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Channel[Task, Repr, Process[Task, Value]] = {
        channel.lift[Task, Repr, Process[Task, Value]] { param =>
          Task.delay(ProcessUtils.runSelect[Value](select.onRecord(param)))
        }
      }

      def executeWithKeyspace[
        Repr <: HList,
        MappedRepr <: HList,
        ReprKeys <: HList
      ](execute: Execute
      )(implicit keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Cluster => Sink[Task, (String, Repr)] = {
        ProcessUtils.forClusterWithKeyspaceAux[Repr, Unit] { param => implicit session =>
          ProcessUtils.runExecute(execute.onRecord(param))
        }
      }

      def selectWithKeyspace[
        Value,
        Repr <: HList,
        MappedRepr <: HList,
        ReprKeys <: HList
      ](select: Select[Value]
      )(implicit keys: Keys.Aux[Repr, ReprKeys],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
        ktl: ToList[ReprKeys, Symbol],
        vtl: ToList[MappedRepr, ParameterValue]
      ): Cluster => Channel[Task, (String, Repr), Process[Task, Value]] = {
        ProcessUtils.forClusterWithKeyspaceAux[Repr, Process[Task, Value]] { param => implicit session =>
          Task.delay(ProcessUtils.runSelect[Value](select.onRecord(param)))
        }
      }
    }

    object params {

      /**
        * Create a stream from parameter lists, which are independently
        * added to a query and executed, ignoring the results.
        *
        * The session is not closed when the stream completes.
        * @param execute The query to add parameters to.
        * @param session
        * @return A stream of ().
        */
      def execute(
        execute: Execute
      )(implicit session: Session
      ): Sink[Task, ParameterList] = {
        sink.lift[Task, Seq[(String, ParameterValue)]] { params =>
          ProcessUtils.runExecute(execute.on(params: _*))
        }
      }

      /**
        * Create a stream from parameter lists, which are independently
        * added to a query and executed, to streams of query results.
        *
        * Use merge.mergeN to run the queries in parallel, or
        * .flatMap(identity) to concatenate them.
        *
        * The session is not closed when the stream completes.
        * @param select The query to add parameters to.
        * @param session
        * @tparam Value
        * @return
        */
      def select[Value](
        select: Select[Value]
      )(implicit session: Session
      ): Channel[Task, ParameterList, Process[Task, Value]] = {
        channel.lift[Task, Seq[(String, ParameterValue)], Process[Task, Value]] { params =>
          Task.delay(ProcessUtils.runSelect[Value](select.on(params: _*)))
        }
      }

      /**
        * Create a stream from keyspace names and parameter lists, which are
        * independently added to a query and executed, ignoring the results.
        *
        * A session is created for each keyspace in the source stream,
        * and they are closed when the stream completes.
        * @param execute
        * @tparam Value
        * @return
        */
      def executeWithKeyspace[Value](
        execute: Execute
      ): Cluster => Sink[Task, (String, ParameterList)] = {
        ProcessUtils.forClusterWithKeyspaceAux[ParameterList, Unit] { params => implicit session =>
          ProcessUtils.runExecute(execute.on(params: _*))
        }
      }

      /**
        * Create a stream from keyspace names and parameter lists, which
        * are independently added to a query and executed, to
        * streams of query results.
        *
        * Use merge.mergeN to run the queries in parallel, or
        * .flatMap(identity) to concatenate them.
        *
        * A session is created for each keyspace in the source stream,
        * and they are closed when the stream completes.
        * @param select
        * @tparam Value
        * @return
        */
      def selectWithKeyspace[Value](
        select: Select[Value]
      ): Cluster => Channel[Task, (String, ParameterList), Process[Task, Value]] = {
        ProcessUtils.forClusterWithKeyspaceAux[ParameterList, Process[Task, Value]] { params => implicit session =>
          Task.delay(ProcessUtils.runSelect[Value](select.on(params: _*)))
        }
      }
    }

    object keys {

      /**
        * Use an instance of Executable to create a stream of queries, whose results are ignored.
        *
        * The session is not closed when the stream completes.
        * @param session
        * @param executable
        * @tparam Key
        * @return
        */
      def execute[Key](
        session: Session
      )(implicit executable: Executable[Key]
      ): Sink[Task, Key] = {
        sink.lift[Task, Key] { key =>
          ProcessUtils.runExecute(executable.execute(key))(session)
        }
      }

      /**
        * Use an instance of Selectable to create a stream of query result streams.
        *
        * Use merge.mergeN on the result to run the queries in parallel, or .flatMap(identity)
        * to concatenate them.
        *
        * The session is not closed when the stream completes.
        * @param session
        * @param selectable
        * @tparam Key
        * @tparam Value
        * @return
        */
      def select[Key, Value](
        session: Session
      )(implicit selectable: Selectable[Key, Value]
      ): Channel[Task, Key, Process[Task, Value]] = {
        channel.lift[Task, Key, Process[Task, Value]] { key =>
          Task.delay(ProcessUtils.runSelect[Value](selectable.select(key))(session))
        }
      }

      /**
        * Use an instance of Executable to create a stream of queries, whose results are ignored.
        *
        * A session is created for the given namespace, which is closed when the stream completes.
        * @param cluster
        * @param keyspace
        * @param executable
        * @tparam Key
        * @return
        */
      def execute[Key](
        cluster: Cluster,
        keyspace: Option[String] = None
      )(implicit executable: Executable[Key]
      ): Sink[Task, Key] = {
        Process.await(ProcessUtils.connect(cluster, keyspace)) { session =>
          execute(session).onComplete(Process.eval_(ProcessUtils.closeSession(session)))
        }
      }

      /**
        * Use an instance of Executable to create a stream of queries, whose results are ignored.
        *
        * A session is created for each keyspace in the source stream,
        * and they are closed when the stream completes.
        * @param cluster
        * @param executable
        * @tparam Key
        * @tparam Value
        * @return
        */
      def executeWithKeyspace[Key, Value](
        cluster: Cluster
      )(implicit executable: Executable[Key]
      ): Sink[Task, (String, Key)] = {
        ProcessUtils.forClusterWithKeyspaceAux[Key, Unit] { key => implicit session =>
          ProcessUtils.runExecute(executable.execute(key))
        }(cluster)
      }

      /**
        * Use an instance of Selectable to create a stream of query result streams.
        *
        * A session is created for the given namespace, which is closed when the stream completes.
        *
        * Use merge.mergeN on the result to run the queries in parallel, or .flatMap(identity)
        * to concatenate them.
        * @param cluster
        * @param keyspace
        * @param selectable
        * @tparam Key
        * @tparam Value
        * @return
        */
      def select[Key, Value](
        cluster: Cluster,
        keyspace: Option[String] = None
      )(implicit selectable: Selectable[Key, Value]
      ): Channel[Task, Key, Process[Task, Value]] = {
        Process.await(ProcessUtils.connect(cluster, keyspace)) { session =>
          select(session).onComplete(Process.eval_(ProcessUtils.closeSession(session)))
        }
      }

      /**
        * Use an instance of Selectable to create a stream of query result streams.
        *
        * A session is created for each keyspace in the source stream,
        * and they are closed when the stream completes.
        *
        * Use merge.mergeN on the result to run the queries in parallel, or .flatMap(identity)
        * to concatenate them.
        *
        * @param cluster
        * @param selectable
        * @tparam Key
        * @tparam Value
        * @return
        */
      def selectWithKeyspace[Key, Value](
        cluster: Cluster
      )(implicit selectable: Selectable[Key, Value]
      ): Channel[Task, (String, Key), Process[Task, Value]] = {
        ProcessUtils.forClusterWithKeyspaceAux[Key, Process[Task, Value]] { key => implicit session =>
          Task.delay(ProcessUtils.runSelect[Value](selectable.select(key)))
        }(cluster)
      }
    }

  }

  object CassandraProcess extends CassandraProcess

  private object ProcessUtils {

    def connect(cluster: Cluster, keyspace: Option[String] = None): Task[Session] = {
      Task.delay(keyspace.map(cluster.connect).getOrElse(cluster.connect()))
    }

    /**
      * Convert a Google future to a Task.
      * @param f
      * @tparam T
      * @return
      */
    def toTask[T](f: ListenableFuture[T]): Task[T] = {
      Task.async[T] { callback =>
        val googleCallback = new FutureCallback[T] {
          override def onFailure(t: Throwable): Unit = {
            callback(DLeft(t))
          }

          override def onSuccess(result: T): Unit = {
            callback(DRight(result))
          }
        }

        Futures.addCallback(f, googleCallback)
      }
    }

    def prepareAsync(
      query: ParameterizedQuery[_]
    )(implicit session: Session
    ): Task[core.PreparedStatement] = {
      toTask(session.prepareAsync(query.queryText))
    }

    def runSelect[Value](
      select: Select[Value]
    )(implicit session: Session
    ): Process[Task, Value] = {
      Process.await(prepareAsync(select).map(p => bind(select, select.queryOptions, p))) { bound =>
        Process.await(runBoundStatement(bound))(_.map(select.converter))
      }
    }

    private def runBoundStatement(
      prepared: PreparedStatement
    )(implicit session: Session
    ): Task[Process[Task, Row]] = {
      toTask[core.ResultSet](session.executeAsync(prepared)).map { result =>
        io.iterator(Task.delay(result.iterator().map(Row.of)))
      }
    }

    def runExecute(
      execute: Execute
    )(implicit session: Session
    ): Task[Unit] = {
      for {
        prepared <- prepareAsync(execute)
        bound = bind(execute, execute.queryOptions, prepared)
        _ <- ignoreBoundStatement(bound)
      } yield ()
    }

    private def ignoreBoundStatement(
      prepared: PreparedStatement
    )(implicit session: Session
    ): Task[Unit] = {
      val rsFuture = session.executeAsync(prepared)
      toTask(rsFuture).map(Function.const(()))
    }

    def closeSession(session: Session): Task[Unit] = {
      val f = session.closeAsync()
      toTask(f).map(Function.const(()))
    }

    /**
      * Run statements, but open only one session per keyspace.
      * Sessions are created when they are first required.
      * @param runner
      * @param cluster
      * @tparam T
      * @tparam O
      * @return
      */
    def forClusterWithKeyspaceAux[T, O](
      runner: T => Session => Task[O]
    )(cluster: Cluster
    ): Channel[Task, (String, T), O] = {

      val sessions = TrieMap.empty[String, Session]

      /**
        * Get a Session for the keyspace, creating it if it does not exist.
        * @param keyspace
        * @return
        */
      def getSession(keyspace: String): Task[Session] = Task.delay {
        sessions.getOrElseUpdate(keyspace, cluster.connect(keyspace))
      }

      /**
        * Empty the sessions collection, and close all the sessions.
        */
      val closeSessions: Task[Unit] = {
        val getToClose = Task.delay[Iterable[Session]] {
          val toClose = sessions.readOnlySnapshot().values
          sessions.clear()
          toClose
        }

        for {
          toClose <- getToClose
          _ <- Task.gatherUnordered(toClose.toSeq.map(closeSession))
        } yield ()
      }

      channel.lift[Task, (String, T), O]{ case (keyspace, thing) =>
        for {
          session <- getSession(keyspace)
          result <- runner(thing)(session)
        } yield result
      }.onComplete(Process.eval_(closeSessions))
    }

  }

}
