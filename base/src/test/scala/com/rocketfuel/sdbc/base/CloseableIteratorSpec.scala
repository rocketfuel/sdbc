package com.rocketfuel.sdbc.base

import java.util.NoSuchElementException
import org.scalatest.FunSuite
import scala.collection.AbstractIterator

class CloseableIteratorSpec extends FunSuite {

  /**
   * Scala < 2.13 uses arithmetic to determine `isEmpty` instead of consuming the iterator,
   * so use something that isn't clever.
   */
  def zeroUntil(maximum: Int): Iterator[Int] =
    new AbstractIterator[Int]() {
      var i = 0
      override def hasNext: Boolean = {
        i < maximum
      }

      override def next(): Int = {
        if (!hasNext) {
          Iterator.empty.next()
        } else {
          val here = i
          i += 1
          here
        }
      }
    }

  class CheckableCloser extends AutoCloseable {
    var isClosed = false
    override def close(): Unit = {
      if (isClosed) {
        throw new Exception("already closed")
      }
      isClosed = true
    }
  }

  def drainHasNext(i: CloseableIterator[_]): Unit = {
    while (i.hasNext) i.next()
  }

  def drainNext(i: CloseableIterator[_]): Unit = {
    try while (true) i.next()
    catch {
      case e: NoSuchElementException =>
    }
  }

  test("closes when not calling hasNext") {
    val closeable = new CheckableCloser

    val i = new CloseableIterator(Iterator(1,2,3), closeable)

    drainNext(i)

    assert(closeable.isClosed)
  }

  test("closes when calling hasNext") {
    val closeable = new CheckableCloser

    val i = new CloseableIterator(Iterator(1,2,3), closeable)

    drainHasNext(i)

    assert(closeable.isClosed)
  }

  test("manually closed at beginning") {
    val closeable = new CheckableCloser

    // Test an iterator that depends on a resource for its elements.
    val original = Iterator.from(0).takeWhile(_ => !closeable.isClosed)

    val i = new CloseableIterator(original, closeable)

    i.close()

    assert(closeable.isClosed)
    assertThrows[NoSuchElementException](i.next())
    assertResult(None)(i.nextOption())
    assert(!i.hasNext)
  }

  test("manually closed at middle") {
    val closeable = new CheckableCloser

    val i = new CloseableIterator(Iterator(1,2,3), closeable)

    i.next()

    i.close()

    assert(closeable.isClosed)
  }

  test("manually closed at end") {
    val closeable = new CheckableCloser

    val i = new CloseableIterator(Iterator(1,2,3), closeable)

    drainHasNext(i)

    i.close()

    assert(closeable.isClosed)
  }

  test("automatically close empty iterator with `next`") {
    val closeable = new CheckableCloser

    val i = new CloseableIterator(Iterator(), closeable)

    drainNext(i)

    assert(closeable.isClosed)
  }

  test("automatically close empty iterator with `hasNext`") {
    val closeable = new CheckableCloser

    val i = new CloseableIterator(Iterator(), closeable)

    drainHasNext(i)

    assert(closeable.isClosed)
  }

  test("span on empty iterator") {
    val closeable = new CheckableCloser

    val i = new CloseableIterator(Iterator(), closeable)

    val (i0, i1) = i.span(_ => true)

    assert(!closeable.isClosed)
    assert(!i0.closer.isClosed)
    assert(!i1.closer.isClosed)

    assert(i0.toSeq.isEmpty)

    assert(closeable.isClosed)
    assert(i0.closer.isClosed)
    assert(i1.closer.isClosed)

    assert(i1.toSeq.isEmpty)
  }

  test("span iterator, left has everything") {
    val closeable = new CheckableCloser

    val i = new CloseableIterator(Iterator(1), closeable)

    val (i0, i1) = i.span(_ => true)

    assertResult(Seq(1))(i0.toSeq)

    assert(closeable.isClosed)
    assert(i0.closer.isClosed)
    assert(i1.closer.isClosed)

    assert(i1.toSeq.isEmpty)
  }

  test("span iterator, right has everything") {
    val closeable = new CheckableCloser

    val i = new CloseableIterator(Iterator(1), closeable)

    val (i0, i1) = i.span(_ => false)

    assertResult(Seq.empty)(i0.toSeq)
//    assert(i1.hasNext)
//
//    assert(!closeable.isClosed)
//    assert(!closer.isClosed)
//    assert(!i0.closer.isClosed)
//    assert(!i1.closer.isClosed)

    assertResult(Seq(1))(i1.toSeq)

    assert(closeable.isClosed)
    assert(i0.closer.isClosed)
    assert(i1.closer.isClosed)
  }

  test("span iterator, both have something") {
    val closeable = new CheckableCloser

    val i = new CloseableIterator(Iterator(1,2), closeable)

    val (i0, i1) = i.span(_ < 2)

    assertResult(Seq(1))(i0.toSeq)

    // I found that consuming `i0` caused `i` to be consumed, which is OK.

    assertResult(Seq(2))(i1.toSeq)

    assert(closeable.isClosed)
    assert(i0.closer.isClosed)
    assert(i1.closer.isClosed)
  }

  test("foreach") {
    val closeable = new CheckableCloser

    val it = new CloseableIterator(zeroUntil(3), closeable)

    var ints = collection.mutable.Buffer.empty[Int]

    for (i <- it) {
      ints += i
    }

    assert(closeable.isClosed)
    assertResult(zeroUntil(3).toSeq)(ints)
  }

  test("filter") {
    val closeable = new CheckableCloser

    val it = new CloseableIterator(zeroUntil(3), closeable)

    var ints = collection.mutable.Set.empty[Int]

    for {
      i <- it
      if i % 2 == 0
    } {
      ints += i
    }

    assert(closeable.isClosed)
    assertResult(Set(0,2))(ints)
  }

  test("drop with not enough available") {
    val closeable = new CheckableCloser

    val it = new CloseableIterator(zeroUntil(3), closeable)

    assert(it.drop(500).toSeq.isEmpty)

    assert(closeable.isClosed)
  }

  test("drop with enough available") {
    val closeable = new CheckableCloser

    val it = new CloseableIterator(zeroUntil(3), closeable)

    assertResult(Seq(1,2))(it.drop(1).toSeq)

    assert(closeable.isClosed)
  }

  test("take with not enough available") {
    val closeable = new CheckableCloser

    val it = new CloseableIterator(zeroUntil(3), closeable)

    assertResult(zeroUntil(3).toSeq)(it.take(3).toSeq)

    assert(closeable.isClosed)
  }

  test("take with enough available") {
    val closeable = new CheckableCloser

    val it = new CloseableIterator(zeroUntil(3), closeable)

    assertResult(zeroUntil(3).toSeq)(it.take(3).toSeq)

    assert(closeable.isClosed)
  }

  test("take with too many available") {
    val closeable = new CheckableCloser

    val it = new CloseableIterator(zeroUntil(3), closeable)

    assertResult(Seq(0))(it.take(1).toSeq)

    assert(!closeable.isClosed)
  }

  test("flatMap closes all") {
    val closables = collection.mutable.Buffer.empty[CheckableCloser]

    def f(i: Int): CloseableIterator[Int] = {
      val closeable = new CheckableCloser
      closables += closeable
      new CloseableIterator(zeroUntil(i), closeable)
    }
    val i = f(3).flatMap(f)

    /*
    0:
    1: 0
    2: 0,1
     */
    assertResult(Seq(0,0,1))(i.toSeq)
    assert(closables.forall(_.isClosed))
  }

}
