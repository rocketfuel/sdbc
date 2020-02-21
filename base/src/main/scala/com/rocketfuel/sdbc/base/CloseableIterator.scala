package com.rocketfuel.sdbc.base

import java.io.Closeable
import scala.collection.{IterableOnce, IterableOnceOps}

/**
 * Has a similar interface to scala.collection.Iterator, but also
 * is Closeable, and combinators return CloseableIterators.
 *
 * The iterator will close itself when you get the last value,
 * but otherwise you'll want to close it.
 *
 * CloseableIterator#buffered is missing.
 * @param underlying
 * @tparam A
 */
class CloseableIterator[+A](
  underlying: Iterator[A],
  private val closer: CloseableIterator.CloseTracking
) extends IterableOnce[A]
  with IterableOnceOps[A, CloseableIterator, CloseableIterator[A]]
  with Closeable {
  self =>

  override def close(): Unit = {
    closer.close()
  }

  def hasNext: Boolean = {
    val hasNext = underlying.hasNext
    if (!hasNext) {
      close()
    }
    hasNext
  }

  def next(): A = underlying.next()

  override def iterator: Iterator[A] = {
    this
  }

  /**
   * An iterator that shares the same close method as the parent. It's usable
   * when the underlying iterator should be closed when `hasNext` fails and
   * there aren't other considerations, like with `span`.
   */
  private def mapped[B](
    underlying: Iterator[B]
  ): CloseableIterator[B] =
    new CloseableIterator[B](underlying, self.closer)

  override def flatten[B](implicit asIterable: A => IterableOnce[B]): CloseableIterator[B] = {
    flatMap(asIterable)
  }

  override def tapEach[U](f: A => U): CloseableIterator[A] = {
    mapped(underlying.tapEach(f))
  }

  override def take(n: Int): CloseableIterator[A] =
    mapped(underlying.take(n))

  override def drop(n: Int): CloseableIterator[A] =
    mapped(underlying.drop(n))

  override def slice(from: Int, until: Int): CloseableIterator[A] =
    mapped(underlying.slice(from, until))

  override def map[B](f: A => B): CloseableIterator[B] =
    mapped[B](underlying.map(f))

  override def flatMap[B](f: A => IterableOnce[B]): CloseableIterator[B] =
    mapped[B](underlying.flatMap(f))

  override def filter(p: A => Boolean): CloseableIterator[A] =
    mapped(underlying.filter(p))

  override def corresponds[B](that: IterableOnce[B])(p: (A, B) => Boolean): Boolean =
    underlying.corresponds(that)(p)

  override def filterNot(p: A => Boolean): CloseableIterator[A] =
    mapped(underlying.filterNot(p))

  override def collect[B](pf: PartialFunction[A, B]): CloseableIterator[B] =
    mapped(underlying.collect(pf))

  override def scanLeft[B](z: B)(op: (B, A) => B): CloseableIterator[B] =
    mapped(underlying.scanLeft(z)(op))

  override def takeWhile(p: A => Boolean): CloseableIterator[A] =
    mapped(underlying.takeWhile(p))

  override def span(p: A => Boolean): (CloseableIterator[A], CloseableIterator[A]) = {
    val (has, hasNot) = underlying.span(p)
    // Span is a special case because we can't let one result close the resource, starving
    // the other result. Instead, wait to call close until the parent is done.
    var hasClosed = false
    var hasNotClosed = false
    val hasTracker = new CloseableIterator.CloseTracking {
      override def close(): Unit = {
        hasClosed = true
        if (hasClosed && hasNotClosed) {
          self.close()
        }
      }
    }
    val hasNotTracker = new CloseableIterator.CloseTracking {
      override def close(): Unit = {
        hasNotClosed = true
        if (hasClosed && hasNotClosed) {
          self.close()
        }
      }
    }

    (new CloseableIterator(has, hasTracker), new CloseableIterator(hasNot, hasNotTracker))
  }

  override def dropWhile(p: A => Boolean): CloseableIterator[A] =
    mapped[A](underlying.dropWhile(p))

  def zip[B](that: CloseableIterator[B]): CloseableIterator[(A, B)] =
    new CloseableIterator(underlying.zip(that.iterator), CloseableIterator.MultiCloseTracking(Seq(closer, that.closer)))

  def zip[B](that: Iterator[B]): CloseableIterator[(A, B)] =
    mapped(underlying.zip(that))

  override def zipWithIndex: CloseableIterator[(A, Int)] =
    mapped(underlying.zipWithIndex)

  def zipAll[B, A1 >: A, B1 >: B](that: Iterator[B], thisElem: A1, thatElem: B1): CloseableIterator[(A1, B1)] =
    mapped[(A1, B1)](underlying.zipAll(that, thisElem, thatElem))

  def zipAll[B, A1 >: A, B1 >: B](that: CloseableIterator[B], thisElem: A1, thatElem: B1): CloseableIterator[(A1, B1)] =
    new CloseableIterator[(A1, B1)](underlying.zipAll(that, thisElem, thatElem), CloseableIterator.MultiCloseTracking(Seq(this.closer, that.closer)))

  def grouped[B >: A](size: Int): CloseableIterator[Seq[B]] =
    mapped(underlying.grouped[B](size))

  def sliding[B >: A](size: Int, step: Int = 1): CloseableIterator[Seq[B]] =
    mapped(underlying.sliding(size, step))

  override def size: Int = underlying.size

  def length: Int = this.size

  def duplicate: (CloseableIterator[A], CloseableIterator[A]) = {
    val (first, second) = underlying.duplicate
    (mapped(first), mapped(second))
  }

  def sameElements(that: Iterator[_]): Boolean =
    underlying.sameElements(that)

  def sameElements(that: CloseableIterator[_]): Boolean =
    underlying.sameElements(that.iterator)

  override def foreach[U](f: (A) => U): Unit = underlying.foreach(f)

  override def isEmpty: Boolean = underlying.isEmpty

  override def forall(p: (A) => Boolean): Boolean = underlying.forall(p)

  override def exists(p: (A) => Boolean): Boolean = underlying.exists(p)

  override def find(p: (A) => Boolean): Option[A] = underlying.find(p)

  override def copyToArray[B >: A](xs: Array[B], start: Int, len: Int): Int = underlying.copyToArray(xs, start, len)

  override def isTraversableAgain: Boolean = underlying.isTraversableAgain

  override def toString: String = underlying.toString

}

object CloseableIterator {
  /**
    * Allow a CloseableIterator to be used in any context requiring
    * a plain Scala Iterator.
    */
  implicit def toIterator[A](i: CloseableIterator[A]): Iterator[A] =
    i.iterator

  trait CloseTracking extends Closeable {
    var isClosed: Boolean = false
  }

  case class SingleCloseTracking(
    closeable: Closeable
  ) extends CloseTracking {
    override def close(): Unit = {
      if (!isClosed) {
        try closeable.close()
        finally isClosed = true
      }
    }
  }

  case class MultiCloseTracking(
    closeables: Seq[CloseTracking]
  ) extends CloseTracking {
    override def close(): Unit = {
      if (!isClosed) {
        try {
          for (closeable <- closeables) {
            closeable.close()
          }
        } finally isClosed = true
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
