package com.rocketfuel.sdbc.cassandra.implementation

import com.rocketfuel.sdbc.base.CompositeParameter
import com.rocketfuel.sdbc.cassandra._
import shapeless.ops.hlist._
import shapeless.ops.record.Keys
import shapeless.{LabelledGeneric, HList}
import scalaz.concurrent.Task
import scalaz.stream._
import ProcessUtils._

trait CassandraProcess {

  /**
   * Create a stream from one query, whose result is ignored.
   * @param execute
   * @param session
   * @return a stream of one () value.
   */
  def execute(execute: Execute)(implicit session: Session): Process[Task, Unit] = {
    Process.eval(runExecute(execute))
  }

  /**
   * Create a stream of values from a query's results.
   * @param select
   * @param session
   * @tparam T
   * @return a stream of the query results.
   */
  def select[T](select: Select[T])(implicit session: Session): Process[Task, T] = {
    runSelect(select)
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
      mapper: Mapper.Aux[CompositeSetter.ToParameterValue.type, Repr, MappedRepr],
      keys: Keys.Aux[Repr, ReprKeys],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Sink[Task, A] = {
      sink.lift[Task, A] {param =>
        runExecute(execute.onProduct(param))
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
      mapper: Mapper.Aux[CompositeSetter.ToParameterValue.type, Repr, MappedRepr],
      keys: Keys.Aux[Repr, ReprKeys],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Channel[Task, A, Process[Task, Value]] = {
      channel.lift[Task, A, Process[Task, Value]] { param =>
        Task.delay(runSelect[Value](select.onProduct(params)))
      }
    }

    def executeWithKeyspace[
      A,
      Repr <: HList,
      MappedRepr <: HList,
      ReprKeys <: HList
    ](execute: Execute
    )(implicit genericA: LabelledGeneric.Aux[A, Repr],
      mapper: Mapper.Aux[CompositeSetter.ToParameterValue.type, Repr, MappedRepr],
      keys: Keys.Aux[Repr, ReprKeys],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Cluster => Sink[Task, (String, A)] = {
      forClusterWithKeyspaceAux[A, Unit] { param => implicit session =>
        runExecute(execute.onProduct(param))
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
      mapper: Mapper.Aux[CompositeSetter.ToParameterValue.type, Repr, MappedRepr],
      keys: Keys.Aux[Repr, ReprKeys],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Cluster => Channel[Task, (String, A), Process[Task, Value]] = {
      forClusterWithKeyspaceAux[A, Process[Task, Value]] { param => implicit session =>
        Task.delay(runSelect[Value](select.onProduct(param)))
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
      mapper: Mapper.Aux[CompositeSetter.ToParameterValue.type, Repr, MappedRepr],
      keys: Keys.Aux[Repr, ReprKeys],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Sink[Task, Repr] = {
      sink.lift[Task, Repr] {param =>
        runExecute(execute.onRecord(param))
      }
    }

    def select[
      Value,
      Repr <: HList,
      MappedRepr <: HList,
      ReprKeys <: HList
    ](select: Select[Value]
    )(implicit session: Session,
      mapper: Mapper.Aux[CompositeSetter.ToParameterValue.type, Repr, MappedRepr],
      keys: Keys.Aux[Repr, ReprKeys],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Channel[Task, Repr, Process[Task, Value]] = {
      channel.lift[Task, Repr, Process[Task, Value]] { param =>
        Task.delay(runSelect[Value](select.onRecord(param)))
      }
    }

    def executeWithKeyspace[
      Repr <: HList,
      MappedRepr <: HList,
      ReprKeys <: HList
    ](execute: Execute
    )(implicit mapper: Mapper.Aux[CompositeSetter.ToParameterValue.type, Repr, MappedRepr],
      keys: Keys.Aux[Repr, ReprKeys],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Cluster => Sink[Task, (String, Repr)] = {
      forClusterWithKeyspaceAux[Repr, Unit] { param => implicit session =>
        runExecute(execute.onRecord(param))
      }
    }

    def selectWithKeyspace[
      Value,
      Repr <: HList,
      MappedRepr <: HList,
      ReprKeys <: HList
    ](select: Select[Value]
    )(implicit mapper: Mapper.Aux[CompositeSetter.ToParameterValue.type, Repr, MappedRepr],
      keys: Keys.Aux[Repr, ReprKeys],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Cluster => Channel[Task, (String, Repr), Process[Task, Value]] = {
      forClusterWithKeyspaceAux[Repr, Process[Task, Value]] { param => implicit session =>
        Task.delay(runSelect[Value](select.onRecord(param)))
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
        runExecute(execute.on(params: _*))
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
        Task.delay(runSelect[Value](select.on(params: _*)))
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
      forClusterWithKeyspaceAux[ParameterList, Unit] { params => implicit session =>
        runExecute(execute.on(params: _*))
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
      forClusterWithKeyspaceAux[ParameterList, Process[Task, Value]] { params => implicit session =>
        Task.delay(runSelect[Value](select.on(params: _*)))
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
        runExecute(executable.execute(key))(session)
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
        Task.delay(runSelect[Value](selectable.select(key))(session))
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
      Process.await(connect(cluster, keyspace)) { session =>
        execute(session).onComplete(Process.eval_(closeSession(session)))
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
      forClusterWithKeyspaceAux[Key, Unit] { key => implicit session =>
        runExecute(executable.execute(key))
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
      Process.await(connect(cluster, keyspace)) { session =>
        select(session).onComplete(Process.eval_(closeSession(session)))
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
      forClusterWithKeyspaceAux[Key, Process[Task, Value]] { key => implicit session =>
        Task.delay(runSelect[Value](selectable.select(key)))
      }(cluster)
    }
  }

}

object CassandraProcess extends CassandraProcess
