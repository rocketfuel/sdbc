package com.rocketfuel.sdbc.base

import cats.effect.Async
import fs2.Stream
import scala.collection.{AbstractIterator, GenTraversableOnce}

/**
 * `CloseableIterator` has a similar interface to scala.collection.Iterator,
 * but also is Closeable, and combinators return CloseableIterators.
 *
 * The iterator will close itself when you get the last value,
 * but otherwise you'll want to close it manually. For instance, calls
 * to `drop` and `take` return a CloseableIterator, but it won't close
 * itself when you fully consume it unless it also happens to fully consume
 * the original iterator.
 *
 * @define closes
 * This method fully consumes the iterator, and so it closes itself.
 * @define doesNotClose
 * This method might not consume the iterator, and so you should close it manually.
 * @define resultCloses
 * If you fully consume the resulting iterator, the resource will be closed.
 * @define resultDoesNotClose
 * If you fully consume the resulting iterator, the resource might not be
 * closed, and so you should close it manually.
 */
class CloseableIterator[+A](
  private val underlying: Iterator[A],
  private[base] val closer: CloseableIterator.CloseTracking
) extends TraversableOnce[A]
  with AutoCloseable {

  override def close(): Unit = {
    closer.close()
  }

  /**
   * For use with child iterators that should not close the resource themselves,
   * but need to report on if it is closed.
   */
  private lazy val reportOnlyCloseTracker = CloseableIterator.ReportOnlyCloseTracker(closer)

  /*
  Make sure that the iterator is closed at the end whether the user is calling
  `hasNext` or not.
   */
  private var calledHasNext = false
  private var _hasNext = false

  def hasNext: Boolean = {
    if (closer.isClosed) {
      return false
    }
    if (!calledHasNext) {
      _hasNext = underlying.hasNext
      calledHasNext = true
      if (!_hasNext) {
        close()
      }
    }
    _hasNext
  }

  def next(): A = {
    if (hasNext) {
      calledHasNext = false
      underlying.next()
    } else throw new NoSuchElementException("next on empty iterator")
  }

  /**
   * Use this iterator wherever you need a [[scala.Iterator]]. You lose the ability
   * to close the resource manually, but if the iterator is consumed it will still
   * close itself.
   */
  override lazy val toIterator: Iterator[A] = {
    new AbstractIterator[A] {
      override def hasNext: Boolean = {
        CloseableIterator.this.hasNext
      }

      override def next(): A = {
        CloseableIterator.this.next()
      }
    }
  }

  override def hasDefiniteSize: Boolean = {
    underlying.hasDefiniteSize
  }

  override def seq: TraversableOnce[A] = this

  override def toTraversable: Traversable[A] = toIterator.toTraversable

  override def toStream: scala.Stream[A] = toIterator.toStream

  def nextOption(): Option[A] = if (hasNext) Some(next()) else None

  /**
   * An iterator that shares the same close method as the parent. It's usable
   * when the underlying iterator should be closed when `hasNext` fails and
   * there aren't other considerations, like with `span`.
   */
  private def mapped[B](
    underlying: Iterator[B]
  ): CloseableIterator[B] =
    new CloseableIterator[B](underlying, closer)

  def map[B](f: A => B): CloseableIterator[B] =
    mapped[B](underlying.map(f))

  /**
   * @note    Reuse: $resultCloses
   */
  def flatMap[B](f: A => GenTraversableOnce[B]): CloseableIterator[B] =
    mapped[B](underlying.flatMap(f))

  /**
   * @note    Reuse: $resultCloses
   */
  def filter(p: A => Boolean): CloseableIterator[A] =
    mapped(underlying.filter(p))

  /**
   * @note    Reuse: $doesNotClose
   */
  def corresponds[B](that: GenTraversableOnce[B])(p: (A, B) => Boolean): Boolean =
    toIterator.corresponds(that)(p)

  /**
   * @note    Reuse: $doesNotClose
   */
  def corresponds[B](that: CloseableIterator[B])(p: (A, B) => Boolean): Boolean =
    toIterator.corresponds(that.toIterator)(p)

  /**
   * @note    Reuse: $resultCloses
   */
  def filterNot(p: A => Boolean): CloseableIterator[A] =
    mapped(underlying.filterNot(p))

  /**
   * @note    Reuse: $resultCloses
   */
  def collect[B](pf: PartialFunction[A, B]): CloseableIterator[B] =
    mapped(underlying.collect(pf))

  /**
   * @note    Reuse: $resultCloses
   */
  def scanLeft[B](z: B)(op: (B, A) => B): CloseableIterator[B] =
    mapped(underlying.scanLeft(z)(op))

  /**
   * @note    Reuse: $resultDoesNotClose
   */
  def takeWhile(p: A => Boolean): CloseableIterator[A] =
    mapped(underlying.takeWhile(p))

  /**
   * @note Consuming the first iterator might close the resource. If not, the second will.
   */
  def span(p: A => Boolean): (CloseableIterator[A], CloseableIterator[A]) = {
    val (has, afterHas) = toIterator.span(p)
    (new CloseableIterator(has, reportOnlyCloseTracker), new CloseableIterator(afterHas, reportOnlyCloseTracker))
  }

  /**
   * @note    Reuse: $resultCloses
   */
  def dropWhile(p: A => Boolean): CloseableIterator[A] =
    mapped[A](underlying.dropWhile(p))

  /**
   * @note    Reuse: $resultDoesNotClose
   */
  def zip[B](that: CloseableIterator[B]): CloseableIterator[(A, B)] =
    mapped(underlying.zip(that.toIterator))

  /**
   * @note    Reuse: $resultDoesNotClose
   */
  def zip[B](that: Iterator[B]): CloseableIterator[(A, B)] =
    mapped(underlying.zip(that))

  /**
   * @note    Reuse: $resultCloses
   */
  def zipWithIndex: CloseableIterator[(A, Int)] =
    mapped(underlying.zipWithIndex)

  /**
   * @note    Reuse: $resultCloses
   */
  def zipAll[B, A1 >: A, B1 >: B](that: Iterator[B], thisElem: A1, thatElem: B1): CloseableIterator[(A1, B1)] =
    mapped[(A1, B1)](underlying.zipAll(that, thisElem, thatElem))

  /**
   * @note    Reuse: $resultCloses
   */
  def zipAll[B, A1 >: A, B1 >: B](that: CloseableIterator[B], thisElem: A1, thatElem: B1): CloseableIterator[(A1, B1)] =
    mapped[(A1, B1)](underlying.zipAll(that.toIterator, thisElem, thatElem))

  /**
   * @note    Reuse: $resultCloses
   */
  def grouped[B >: A](size: Int): CloseableIterator[Seq[B]] =
    mapped(underlying.grouped[B](size))

  /**
   * @note    Reuse: $resultCloses
   */
  def sliding[B >: A](size: Int, step: Int = 1): CloseableIterator[Seq[B]] =
    mapped(underlying.sliding(size, step))

  /**
   * @note    Reuse: $closes
   */
  override def size: Int = underlying.size

  /**
   * @note    Reuse: $closes
   */
  def length: Int = this.size

  /**
   * @note    Reuse: $resultCloses
   */
  def duplicate: (CloseableIterator[A], CloseableIterator[A]) = {
    val (first, second) = toIterator.duplicate
    (new CloseableIterator[A](first, reportOnlyCloseTracker), new CloseableIterator[A](second, reportOnlyCloseTracker))
  }

  /**
   * @note    Reuse: $doesNotClose
   */
  def sameElements(that: Iterator[_]): Boolean =
    toIterator.sameElements(that)

  /**
   * @note    Reuse: $doesNotClose
   */
  def sameElements(that: CloseableIterator[_]): Boolean =
    toIterator.sameElements(that.toIterator)

  /**
   * @note    Reuse: $closes
   */
  override def foreach[U](f: (A) => U): Unit = toIterator.foreach(f)

  override def isEmpty: Boolean = !hasNext

  /**
   * @note    Reuse: $doesNotClose
   */
  override def forall(p: (A) => Boolean): Boolean = toIterator.forall(p)

  /**
   * @note    Reuse: $doesNotClose
   */
  override def exists(p: (A) => Boolean): Boolean = toIterator.exists(p)

  /**
   * @note    Reuse: $doesNotClose
   */
  override def find(p: (A) => Boolean): Option[A] = toIterator.find(p)

  /**
   * @note    Reuse: $doesNotClose
   */
  override def copyToArray[B >: A](xs: Array[B], start: Int, len: Int): Unit = toIterator.copyToArray(xs, start, len)

  override def isTraversableAgain: Boolean = underlying.isTraversableAgain

  override def toString: String = "<closeable iterator>"

}

object CloseableIterator {
  def toStream[F[x], A](i: F[CloseableIterator[A]])(implicit a: Async[F]): fs2.Stream[F, A] = {
    fs2.Stream.bracket[F, CloseableIterator[A]](i)(i => a.delay(i.close())).flatMap(i => Stream.fromIterator[F](i.toIterator))
  }

  trait CloseTracking extends AutoCloseable {
    protected var _isClosed: Boolean = false
    def isClosed: Boolean = _isClosed
  }

  case class ReportOnlyCloseTracker(reportee: CloseTracking) extends CloseTracking {
    override def close(): Unit = {
      // Don't actually close anything. The parent iterator will close when
      // either of the children finish iterating through it, so just report
      // the parent's status.
    }

    override def isClosed: Boolean = reportee.isClosed
  }

  case class EmptyCloseTracking() extends CloseTracking {
    override def close(): Unit = {
      _isClosed = true
    }
  }

  case class SingleCloseTracking(
    closeable: AutoCloseable
  ) extends CloseTracking {
    override def close(): Unit = {
      if (!_isClosed) {
        try closeable.close()
        finally _isClosed = true
      }
    }
  }

  /**
   * For use when the iterator has multiple underlying resources.
   * @param closeables
   */
  case class MultiCloseTracking(
    closeables: Seq[CloseTracking]
  ) extends CloseTracking {
    override def close(): Unit = {
      if (!_isClosed) {
        try {
          for (closeable <- closeables) {
            try {}
            finally closeable.close()
          }
        } finally _isClosed = true
      }
    }
  }

  /**
   * Call close on `c` but only when `dependencies` are also closed.
   */
  case class DependentCloseTracking(
    dependencies: Seq[CloseTracking],
    c: CloseTracking
  ) extends CloseTracking {
    override def close(): Unit = {
      if (dependencies.forall(_.isClosed)) {
        c.close()
      }
    }
  }
}
