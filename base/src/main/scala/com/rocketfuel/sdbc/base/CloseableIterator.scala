package com.rocketfuel.sdbc.base

import java.io.Closeable
import scala.collection.GenTraversableOnce

/**
  * Has a similar interface to scala.collection.Iterator, but also
  * is Closeable, and combinators return CloseableIterators.
  *
  * CloseableIterator#buffered is missing.
  * @param underlying
  * @tparam A
  */
abstract class CloseableIterator[+A](
  underlying: Iterator[A]
) extends TraversableOnce[A]
  with Closeable {
  self =>

  /**
    * An iterator that shares the same close method as the parent.
    * @param underlying
    * @tparam B
    */
  private class Mapped[B](
    underlying: Iterator[B]
  ) extends CloseableIterator(underlying) {
    override def close(): Unit = self.close()
  }

  def hasNext: Boolean = underlying.hasNext

  def next(): A = underlying.next()

  def take(n: Int): CloseableIterator[A] =
    new Mapped(underlying.take(n))

  def drop(n: Int): CloseableIterator[A] = {
    new Mapped(underlying.drop(n))
  }

  def slice(from: Int, until: Int): CloseableIterator[A] =
    new Mapped(underlying.slice(from, until))

  def map[B](f: A => B): CloseableIterator[B] =
    new Mapped[B](underlying.map(f))

  def ++[B >: A](that: GenTraversableOnce[B]): CloseableIterator[B] =
    new Mapped[B](underlying ++ that)

  def flatMap[B](f: A => GenTraversableOnce[B]): CloseableIterator[B] =
    new Mapped[B](underlying.flatMap(f))

  def filter(p: A => Boolean): CloseableIterator[A] =
    new Mapped(underlying.filter(p))

  def corresponds[B](that: GenTraversableOnce[B])(p: (A, B) => Boolean): Boolean =
    underlying.corresponds(that)(p)

  def withFilter(p: A => Boolean): CloseableIterator[A] = filter(p)

  def filterNot(p: A => Boolean): CloseableIterator[A] = filter(!p(_))

  def collect[B](pf: PartialFunction[A, B]): CloseableIterator[B] =
    new Mapped(underlying.collect(pf))

  def scanLeft[B](z: B)(op: (B, A) => B): CloseableIterator[B] =
    new Mapped(underlying.scanLeft(z)(op))

  def scanRight[B](z: B)(op: (A, B) => B): CloseableIterator[B] =
    new Mapped(underlying.scanRight(z)(op))

  def takeWhile(p: A => Boolean): CloseableIterator[A] =
    new Mapped(underlying.takeWhile(p))

  def partition(p: A => Boolean): (CloseableIterator[A], CloseableIterator[A]) = {
    val (has, hasNot) = underlying.partition(p)
    (new Mapped(has), new Mapped(hasNot))
  }

  def span(p: A => Boolean): (CloseableIterator[A], CloseableIterator[A]) = {
    val (has, hasNot) = underlying.span(p)
    (new Mapped(has), new Mapped(hasNot))
  }

  def dropWhile(p: A => Boolean): CloseableIterator[A] =
    new Mapped[A](underlying.dropWhile(p))

  def zip[B](that: CloseableIterator[B]): CloseableIterator[(A, B)] =
    new CloseableIterator(underlying.zip(that.toIterator)) {
      override def close(): Unit = {
        self.close()
        that.close()
      }
    }

  def zip[B](that: Iterator[B]): CloseableIterator[(A, B)] =
    new Mapped(underlying.zip(that))

  def padTo[A1 >: A](len: Int, elem: A1): CloseableIterator[A1] =
    new Mapped(underlying.padTo(len, elem))

  def zipWithIndex: CloseableIterator[(A, Int)] =
    new Mapped(underlying.zipWithIndex)

  def zipAll[B, A1 >: A, B1 >: B](that: CloseableIterator[B], thisElem: A1, thatElem: B1): CloseableIterator[(A1, B1)] =
    new Mapped[(A1, B1)](underlying.zipAll(that.toIterator, thisElem, thatElem))

  def indexWhere(p: A => Boolean): Int =
    underlying.indexWhere(p)

  def indexOf[B >: A](elem: B): Int =
    underlying.indexOf(elem)

  def grouped[B >: A](size: Int): CloseableIterator[Seq[B]] =
    new Mapped(underlying.grouped[B](size))

  def sliding[B >: A](size: Int, step: Int = 1): CloseableIterator[Seq[B]] =
    new Mapped(underlying.sliding(size, step))

  def length: Int = this.size

  def duplicate: (CloseableIterator[A], CloseableIterator[A]) = {
    val (first, second) = underlying.duplicate
    (new Mapped(first), new Mapped(second))
  }

  def patch[B >: A](from: Int, patchElems: CloseableIterator[B], replaced: Int): CloseableIterator[B] =
    new CloseableIterator[B](underlying.patch(from, patchElems.toIterator, replaced)) {
      override def close(): Unit = {
        self.close()
        patchElems.close()
      }
    }

  def patch[B >: A](from: Int, patchElems: Iterator[B], replaced: Int): CloseableIterator[B] =
    new Mapped(underlying.patch(from, patchElems, replaced))

  def sameElements(that: Iterator[_]): Boolean =
    underlying.sameElements(that)

  def sameElements(that: CloseableIterator[_]): Boolean =
    underlying.sameElements(that.toIterator)

  override def foreach[U](f: (A) => U): Unit = underlying.foreach(f)

  override def isEmpty: Boolean = underlying.isEmpty

  override def hasDefiniteSize: Boolean = underlying.hasDefiniteSize

  override def seq: TraversableOnce[A] = underlying.seq

  override def forall(p: (A) => Boolean): Boolean = underlying.forall(p)

  override def exists(p: (A) => Boolean): Boolean = underlying.exists(p)

  override def find(p: (A) => Boolean): Option[A] = underlying.find(p)

  override def copyToArray[B >: A](xs: Array[B], start: Int, len: Int): Unit = underlying.copyToArray(xs, start, len)

  override def toTraversable: Traversable[A] = underlying.toTraversable

  override def isTraversableAgain: Boolean = underlying.isTraversableAgain

  override def toStream: Stream[A] = underlying.toStream

  override def toIterator: Iterator[A] = underlying

  override def toString: String = underlying.toString

}

object CloseableIterator {
  /**
    * Allow a CloseableIterator to be used in any context requiring
    * a plain Scala Iterator.
    */
  implicit def toIterator[A](i: CloseableIterator[A]): Iterator[A] =
    i.toIterator
}
