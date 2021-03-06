package com.rocketfuel.sdbc.base

import cats.effect.Async
import fs2.Stream
import scala.collection.{AbstractIterator, IterableOnceOps}

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
class CloseableIterator[+A] private (
  private val underlying: Iterator[A],
  private[base] val closer: CloseableIterator.CloseTracking
) extends IterableOnce[A]
  with IterableOnceOps[A, CloseableIterator, CloseableIterator[A]]
  with AutoCloseable {

  def this(original: Iterator[A], resource: AutoCloseable) {
    this(original, CloseableIterator.CloseTracking(original, resource))
  }

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
      closer.closeIfConsumed()
    }
    _hasNext
  }

  def next(): A = {
    if (hasNext) {
      calledHasNext = false
      underlying.next()
    } else Iterator.empty.next()
  }

  /**
   * Use this iterator wherever you need a [[scala.Iterator]]. You lose the ability
   * to close the resource manually, but if the iterator is consumed it will still
   * close itself.
   */
  override lazy val iterator: Iterator[A] = {
    new AbstractIterator[A] {
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
   * An iterator that shares the same close method as the parent. It closes
   * only if the parent is consumed.
   */
  private def mapped[B](
    mappedUnderlying: Iterator[B]
  ): CloseableIterator[B] =
    new CloseableIterator[B](mappedUnderlying, closer)

  /**
   * @note    Reuse: $resultCloses
   */
  override def flatten[B](implicit asIterable: A => IterableOnce[B]): CloseableIterator[B] = {
    flatMap(asIterable)
  }

  /**
   * @note    Reuse: $resultCloses
   */
  override def tapEach[U](f: A => U): CloseableIterator[A] = {
    mapped(underlying.tapEach(f))
  }

  /**
   * @note    Reuse: $resultDoesNotClose
   */
  override def take(n: Int): CloseableIterator[A] =
    mapped(underlying.take(n))

  /**
   * @note    Reuse: $resultCloses
   */
  override def drop(n: Int): CloseableIterator[A] = {
    mapped(underlying.drop(n))
  }

  /**
   * @note    Reuse: $resultDoesNotClose
   */
  override def slice(from: Int, until: Int): CloseableIterator[A] =
    mapped(underlying.slice(from, until))

  /**
   * @note    Reuse: $resultCloses
   */
  override def map[B](f: A => B): CloseableIterator[B] =
    mapped[B](underlying.map(f))

  /**
   * @note    Reuse: $resultCloses
   */
  override def flatMap[B](f: A => IterableOnce[B]): CloseableIterator[B] =
    mapped[B](underlying.flatMap(f))

  /**
   * @note    Reuse: $resultCloses
   */
  override def filter(p: A => Boolean): CloseableIterator[A] =
    mapped(underlying.filter(p))

  /**
   * @note    Reuse: $doesNotClose
   */
  override def corresponds[B](that: IterableOnce[B])(p: (A, B) => Boolean): Boolean =
    iterator.corresponds(that)(p)

  /**
   * @note    Reuse: $doesNotClose
   */
  def corresponds[B](that: CloseableIterator[B])(p: (A, B) => Boolean): Boolean =
    iterator.corresponds(that.iterator)(p)

  /**
   * @note    Reuse: $resultCloses
   */
  override def filterNot(p: A => Boolean): CloseableIterator[A] =
    mapped(underlying.filterNot(p))

  /**
   * @note    Reuse: $resultCloses
   */
  override def collect[B](pf: PartialFunction[A, B]): CloseableIterator[B] =
    mapped(underlying.collect(pf))

  /**
   * @note    Reuse: $resultCloses
   */
  override def scanLeft[B](z: B)(op: (B, A) => B): CloseableIterator[B] =
    mapped(underlying.scanLeft(z)(op))

  /**
   * @note    Reuse: $resultDoesNotClose
   */
  override def takeWhile(p: A => Boolean): CloseableIterator[A] =
    mapped(underlying.takeWhile(p))

  /**
   * @note Consuming the first iterator might close the resource. If not, the second will.
   */
  override def span(p: A => Boolean): (CloseableIterator[A], CloseableIterator[A]) = {
    val (has, afterHasNot) = iterator.span(p)
    (mapped(has), mapped(afterHasNot))
  }

  /**
   * @note    Reuse: $resultCloses
   */
  override def dropWhile(p: A => Boolean): CloseableIterator[A] =
    mapped[A](underlying.dropWhile(p))

  /**
   * @note    Reuse: $resultDoesNotClose
   */
  def zip[B](that: CloseableIterator[B]): CloseableIterator[(A, B)] =
    mapped(underlying.zip(that))

  /**
   * @note    Reuse: $resultDoesNotClose
   */
  def zip[B](that: Iterator[B]): CloseableIterator[(A, B)] =
    mapped(underlying.zip(that))

  /**
   * @note    Reuse: $resultCloses
   */
  override def zipWithIndex: CloseableIterator[(A, Int)] =
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
    mapped[(A1, B1)](underlying.zipAll(that, thisElem, thatElem))

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
  override def size: Int = iterator.size

  /**
   * @note    Reuse: $closes
   */
  def length: Int = this.size

  /**
   * @note    Reuse: $resultCloses
   */
  def duplicate: (CloseableIterator[A], CloseableIterator[A]) = {
    val (first, second) = iterator.duplicate
    (mapped(first), mapped(second))
  }

  /**
   * @note    Reuse: $doesNotClose
   */
  def sameElements(that: Iterator[_]): Boolean =
    iterator.sameElements(that)

  /**
   * @note    Reuse: $doesNotClose
   */
  def sameElements(that: CloseableIterator[_]): Boolean =
    iterator.sameElements(that)

  /**
   * @note    Reuse: $closes
   */
  override def foreach[U](f: (A) => U): Unit = iterator.foreach(f)

  override def isEmpty: Boolean = !hasNext

  /**
   * @note    Reuse: $doesNotClose
   */
  override def forall(p: (A) => Boolean): Boolean = iterator.forall(p)

  /**
   * @note    Reuse: $doesNotClose
   */
  override def exists(p: (A) => Boolean): Boolean = iterator.exists(p)

  /**
   * @note    Reuse: $doesNotClose
   */
  override def find(p: (A) => Boolean): Option[A] = iterator.find(p)

  /**
   * @note    Reuse: $doesNotClose
   */
  override def copyToArray[B >: A](xs: Array[B], start: Int, len: Int): Int = iterator.copyToArray(xs, start, len)

  override def isTraversableAgain: Boolean = underlying.isTraversableAgain

  override def toString: String = "<closeable iterator>"

}

object CloseableIterator {
  def toStream[F[x], A](i: F[CloseableIterator[A]])(implicit a: Async[F]): fs2.Stream[F, A] = {
    fs2.Stream.bracket[F, CloseableIterator[A]](i)(i => a.delay(i.close())).flatMap(i => Stream.fromIterator[F](i.iterator))
  }

  private val _empty: CloseableIterator[Nothing] = {
    val i = new CloseableIterator(Iterator.empty, CloseTracking(Iterator.empty, new AutoCloseable {
      override def close(): Unit = ()
    }))
    i.close()
    i
  }

  def empty[A]: CloseableIterator[A] = _empty

  case class CloseTracking(
    original: Iterator[_],
    resource: AutoCloseable
  ) extends AutoCloseable {

    override def close(): Unit = {
      if (!_isClosed) {
        _isClosed = true
        resource.close()
      }
    }

    def closeIfConsumed(): Unit = {
      if (!original.hasNext) {
        close()
      }
    }

    protected var _isClosed: Boolean = false
    def isClosed: Boolean = _isClosed
  }
}
