package com.rocketfuel.sdbc.base

import cats.effect.Async
import fs2.Stream

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
  private val underlying: Iterator[A],
  private val closer: CloseableIterator.CloseTracking
) extends IterableOnce[A]
  with IterableOnceOps[A, CloseableIterator, CloseableIterator[A]]
  with AutoCloseable {
  self =>

  override def close(): Unit = {
    closer.close()
  }

  /*
  Make sure that the iterator is closed at the end whether the user is calling
  `hasNext` or not.
   */
  private var calledHasNext = false
  private var _hasNext = false

  def hasNext: Boolean = {
    if (!calledHasNext) {
      _hasNext = underlying.hasNext
      calledHasNext = true
    }
    if (!_hasNext) {
      close()
    }
    _hasNext
  }

  def next(): A = {
    if (hasNext) {
      calledHasNext = false
      underlying.next()
    } else throw new NoSuchElementException("next on empty iterator")
  }

  override def iterator: Iterator[A] = {
    // Don't just use the underlying iterator, because it won't call `close` at the end.
    new Iterator[A] {
      override def hasNext: Boolean = {
        CloseableIterator.this.hasNext
      }

      override def next(): A = {
        CloseableIterator.this.next()
      }
    }
  }

  def nextOption(): Option[A] = if (hasNext) Some(next()) else None

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

  def corresponds[B](that: CloseableIterator[B])(p: (A, B) => Boolean): Boolean = {
    try underlying.corresponds(that)(p)
    finally {
      try close()
      finally that.close()
    }
  }

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
    (new CloseableIterator(has, closer), new CloseableIterator(hasNot, closer))
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
  def toStream[F[x], A](i: F[CloseableIterator[A]])(implicit a: Async[F]): fs2.Stream[F, A] = {
    fs2.Stream.bracket[F, CloseableIterator[A]](i)(i => a.delay(i.close())).flatMap(i => Stream.fromIterator[F](i.iterator))
  }

  trait CloseTracking extends AutoCloseable {
    protected var _isClosed: Boolean = false
    def isClosed: Boolean = _isClosed
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
