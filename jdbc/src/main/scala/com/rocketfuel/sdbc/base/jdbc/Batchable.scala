package com.rocketfuel.sdbc.base.jdbc

import fs2.{Pipe, Stream}
import fs2.util.Async

trait Batchable {
  self: DBMS with Connection =>

  trait Batchable[Key] {
    def batch(key: Key): Batch
  }

  object Batchable {
    def apply[Key](implicit b: Batchable[Key]): Batchable[Key] = b

    def apply[Key](f: Key => Batch): Batchable[Key] =
      new Batchable[Key] {
        override def batch(key: Key): Batch =
          f(key)
      }

    def batch[Key](
      key: Key
    )(implicit batchable: Batchable[Key],
      connection: Connection
    ): Batch.Results = {
      batchable.batch(key).batch()
    }

    def stream[F[_], Key](
      key: Key
    )(implicit batchable: Batchable[Key],
      pool: Pool,
      async: Async[F]
    ): Stream[F, Batch.Result] =
      batchable.batch(key).stream()

    def query[TC <: Queryable[Q, Key], Q <: CompiledParameterizedQuery[Q], Key](
      keys: Vector[Key]
    )(implicit q: TC,
      connection: Connection
    ): Batch.Results = {
      Batch.batch(keys.map(q.query): _*)
    }

    def pipe[
      F[_],
      TC <: Queryable[Q, Key],
      Q <: CompiledParameterizedQuery[Q],
      Key
    ](implicit async: Async[F],
      q: TC,
      pool: Pool
    ): Pipe[F, Key, Batch.Result] =
      (keys: Stream[F, Key]) =>
        keys.map(q.query).through(Batch.pipe)

//    def delete[Key](
//      keys: Seq[Key]
//    )(implicit d: Deletable[Key],
//      connection: Connection
//    ): Map[CompiledStatement, IndexedSeq[Long]] = {
//      query[Deletable[Key], Delete, Key](keys)
//    }
//
//    def deletePipe[F[_], Key](
//      n: Int,
//      allowFewer: Boolean = true
//    )(implicit async: Async[F],
//      d: Deletable[Key],
//      pool: Pool
//    ): Pipe[F, Key, Stream[F, (CompiledStatement, IndexedSeq[Long])]] =
//      queryPipe[F, Deletable[Key], Delete, Key](n, allowFewer)
//
//    def ignore[Key](
//      keys: Seq[Key]
//    )(implicit i: Ignorable[Key],
//      connection: Connection
//    ): Map[CompiledStatement, IndexedSeq[Long]] = {
//      query[Ignorable[Key], Ignore, Key](keys)
//    }
//
//    def ignorePipe[F[_], Key](
//      n: Int,
//      allowFewer: Boolean = true
//    )(implicit async: Async[F],
//      i: Ignorable[Key],
//      pool: Pool
//    ): Pipe[F, Key, Stream[F, (CompiledStatement, IndexedSeq[Long])]] =
//      queryPipe[F, Ignorable[Key], Ignore, Key](n, allowFewer)
//
//    def insert[Key](
//      keys: Seq[Key]
//    )(implicit i: Insertable[Key],
//      connection: Connection
//    ): Map[CompiledStatement, IndexedSeq[Long]] = {
//      query[Insertable[Key], Insert, Key](keys)
//    }
//
//    def insertPipe[F[_], Key](
//      n: Int,
//      allowFewer: Boolean = true
//    )(implicit async: Async[F],
//      i: Insertable[Key],
//      pool: Pool
//    ): Pipe[F, Key, Stream[F, (CompiledStatement, IndexedSeq[Long])]] =
//      queryPipe[F, Insertable[Key], Insert, Key](n, allowFewer)
//
//    def select[Key, Result](
//      keys: Seq[Key]
//    )(implicit s: Selectable[Key, Result],
//      connection: Connection
//    ): Map[CompiledStatement, IndexedSeq[Long]] = {
//      query[Selectable[Key, Result], Select[Result], Key](keys)
//    }
//
//    def selectPipe[F[_], Key, Result](
//      n: Int,
//      allowFewer: Boolean = true
//    )(implicit async: Async[F],
//      s: Selectable[Key, Result],
//      pool: Pool
//    ): Pipe[F, Key, Stream[F, (CompiledStatement, IndexedSeq[Long])]] =
//      queryPipe[F, Selectable[Key, Result], Select[Result], Key](n, allowFewer)
//
//    def selectForUpdate[Key](
//      keys: Seq[Key]
//    )(implicit s: SelectForUpdatable[Key],
//      connection: Connection
//    ): Map[CompiledStatement, IndexedSeq[Long]] = {
//      query[SelectForUpdatable[Key], SelectForUpdate, Key](keys)
//    }
//
//    def selectForUpdatePipe[F[_], Key](
//      n: Int,
//      allowFewer: Boolean = true
//    )(implicit async: Async[F],
//      s: SelectForUpdatable[Key],
//      pool: Pool
//    ): Pipe[F, Key, Stream[F, (CompiledStatement, IndexedSeq[Long])]] =
//      queryPipe[F, SelectForUpdatable[Key], SelectForUpdate, Key](n, allowFewer)
//
//    def update[Key](
//      keys: Seq[Key]
//    )(implicit u: Updatable[Key],
//      connection: Connection
//    ): Map[CompiledStatement, IndexedSeq[Long]] = {
//      query[Updatable[Key], Update, Key](keys)
//    }
//
//    def updatePipe[F[_], Key](
//      n: Int,
//      allowFewer: Boolean = true
//    )(implicit async: Async[F],
//      u: Updatable[Key],
//      pool: Pool
//    ): Pipe[F, Key, Stream[F, (CompiledStatement, IndexedSeq[Long])]] =
//      queryPipe[F, Updatable[Key], Update, Key](n, allowFewer)

    trait syntax {
      implicit class BatchSyntax[Key](key: Key)(implicit batchable: Batchable[Key]) {
        def batch()(implicit connection: Connection): Batch.Results =
          Batchable.batch(key)

        def batchStream[
          F[_]
        ]()(implicit pool: Pool,
          async: Async[F]
        ): Stream[F, Batch.Result] =
          Batchable.stream(key)
      }

      implicit class BatchQuerySyntax[
        TC <: Queryable[Q, Key],
        Q <: CompiledParameterizedQuery[Q],
        Key
      ](keys: Seq[Key]
      )(implicit tc: TC,
        q: Queryable[Q, Key]
      ) {
        def batches()(implicit connection: Connection): Batch.Results = {
          Batchable.query[TC, Q, Key](keys.toVector)
        }

        def streams[
          F[_]
        ](implicit async: Async[F],
          pool: Pool
        ): Stream[F, Batch.Result] = {
          Stream[F, Key](keys: _*).through(Batchable.pipe[F, TC, Q, Key])
        }
      }
//
//      implicit class BatchDeleteSyntax[Key](keys: Seq[Key])(implicit d: Deletable[Key]) {
//        def delete()(
//          implicit connection: Connection
//        ): Map[CompiledStatement, IndexedSeq[Long]] = {
//          Batchable.delete(keys)
//        }
//
//        def deleteStream[F[_]](
//          n: Int,
//          allowFewer: Boolean = true
//        )(implicit async: Async[F],
//          pool: Pool
//        ): Stream[F, Stream[F, (CompiledStatement, IndexedSeq[Long])]] = {
//          Stream[F, Key](keys: _*).through(Batchable.deletePipe(n, allowFewer))
//        }
//      }
//
//      implicit class BatchIgnoreSyntax[Key](keys: Seq[Key])(implicit i: Ignorable[Key]) {
//        def ignore()(implicit connection: Connection): Map[CompiledStatement, IndexedSeq[Long]] = {
//          Batchable.ignore(keys)
//        }
//
//        def ignoreStream[F[_]](
//          n: Int,
//          allowFewer: Boolean = true
//        )(implicit async: Async[F],
//          pool: Pool
//        ): Stream[F, Stream[F, (CompiledStatement, IndexedSeq[Long])]] = {
//          Stream[F, Key](keys: _*).through(Batchable.ignorePipe(n, allowFewer))
//        }
//      }
//
//      implicit class BatchInsertSyntax[Key](keys: Seq[Key])(implicit i: Insertable[Key]) {
//        def insert()(implicit connection: Connection): Map[CompiledStatement, IndexedSeq[Long]] = {
//          Batchable.insert(keys)
//        }
//
//        def insertStream[F[_]](
//          n: Int,
//          allowFewer: Boolean = true
//        )(implicit async: Async[F],
//          pool: Pool
//        ): Stream[F, Stream[F, (CompiledStatement, IndexedSeq[Long])]] = {
//          Stream[F, Key](keys: _*).through(Batchable.insertPipe(n, allowFewer))
//        }
//      }
//
//      implicit class BatchSelectSyntax[Key, Result](keys: Seq[Key])(implicit s: Selectable[Key, Result]) {
//        def select()(
//          implicit connection: Connection
//        ): Map[CompiledStatement, IndexedSeq[Long]] = {
//          query[Selectable[Key, Result], Select[Result], Key](keys)
//        }
//
//        def selectStream[F[_]](
//          n: Int,
//          allowFewer: Boolean = true
//        )(implicit async: Async[F],
//          pool: Pool
//        ): Stream[F, Stream[F, (CompiledStatement, IndexedSeq[Long])]] = {
//          Stream[F, Key](keys: _*).through(Batchable.selectPipe(n, allowFewer))
//        }
//      }
//
//      implicit class BatchSelectForUpdateSyntax[Key](keys: Seq[Key])(implicit s: SelectForUpdatable[Key]) {
//        def selectForUpdate()(implicit connection: Connection): Map[CompiledStatement, IndexedSeq[Long]] = {
//          Batchable.selectForUpdate[Key](keys)
//        }
//
//        def selectForUpdateStream[F[_]](
//          n: Int,
//          allowFewer: Boolean = true
//        )(implicit async: Async[F],
//          pool: Pool
//        ): Stream[F, Stream[F, (CompiledStatement, IndexedSeq[Long])]] = {
//          Stream[F, Key](keys: _*).through(Batchable.selectForUpdatePipe(n, allowFewer))
//        }
//      }
//
//      implicit class BatchUpdateSyntax[Key](keys: Seq[Key])(implicit u: Updatable[Key]) {
//        def update()(implicit connection: Connection): Map[CompiledStatement, IndexedSeq[Long]] = {
//          Batchable.update(keys)
//        }
//
//        def updateStream[F[_]](
//          n: Int,
//          allowFewer: Boolean = true
//        )(implicit async: Async[F],
//          pool: Pool
//        ): Stream[F, Stream[F, (CompiledStatement, IndexedSeq[Long])]] = {
//          Stream[F, Key](keys: _*).through(Batchable.updatePipe(n, allowFewer))
//        }
//      }
    }

    object syntax extends syntax
  }

}
