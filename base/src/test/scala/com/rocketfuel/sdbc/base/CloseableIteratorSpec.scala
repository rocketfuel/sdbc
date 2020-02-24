package com.rocketfuel.sdbc.base

import java.util.NoSuchElementException
import org.scalatest.FunSuite

class CloseableIteratorSpec extends FunSuite {

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

    val closer = CloseableIterator.SingleCloseTracking(closeable)

    val i = new CloseableIterator(Iterator(1,2,3), closer)

    drainNext(i)

    assert(closeable.isClosed)
    assert(closer.isClosed)
  }

  test("closes when calling hasNext") {
    val closeable = new CheckableCloser

    val closer = CloseableIterator.SingleCloseTracking(closeable)

    val i = new CloseableIterator(Iterator(1,2,3), closer)

    drainHasNext(i)

    assert(closeable.isClosed)
    assert(closer.isClosed)
  }

  test("manually closed at beginning") {
    val closeable = new CheckableCloser

    val closer = CloseableIterator.SingleCloseTracking(closeable)

    val i = new CloseableIterator(Iterator(1,2,3), closer)

    i.close()

    assert(closeable.isClosed)
    assert(closer.isClosed)
  }

  test("manually closed at middle") {
    val closeable = new CheckableCloser

    val closer = CloseableIterator.SingleCloseTracking(closeable)

    val i = new CloseableIterator(Iterator(1,2,3), closer)

    i.next()

    i.close()

    assert(closeable.isClosed)
    assert(closer.isClosed)
  }

  test("manually closed at end") {
    val closeable = new CheckableCloser

    val closer = CloseableIterator.SingleCloseTracking(closeable)

    val i = new CloseableIterator(Iterator(1,2,3), closer)

    drainHasNext(i)

    i.close()

    assert(closeable.isClosed)
    assert(closer.isClosed)
  }

  test("automatically close empty iterator with `next`") {
    val closeable = new CheckableCloser

    val closer = CloseableIterator.SingleCloseTracking(closeable)

    val i = new CloseableIterator(Iterator(), closer)

    drainNext(i)

    assert(closeable.isClosed)
    assert(closer.isClosed)
  }

  test("automatically close empty iterator with `hasNext`") {
    val closeable = new CheckableCloser

    val closer = CloseableIterator.SingleCloseTracking(closeable)

    val i = new CloseableIterator(Iterator(), closer)

    drainHasNext(i)

    assert(closeable.isClosed)
    assert(closer.isClosed)
  }

  test("span on empty iterator") {
    val closeable = new CheckableCloser

    val closer = CloseableIterator.SingleCloseTracking(closeable)

    val i = new CloseableIterator(Iterator(), closer)

    val (i0, i1) = i.span(_ => true)

    assert(!closeable.isClosed)
    assert(!closer.isClosed)
    assert(!i0.closer.isClosed)
    assert(!i1.closer.isClosed)

    drainHasNext(i0)

    assert(closeable.isClosed)
    assert(closer.isClosed)
    assert(i0.closer.isClosed)
    assert(i1.closer.isClosed)
  }

  test("span iterator, left has everything") {
    val closeable = new CheckableCloser

    val closer = CloseableIterator.SingleCloseTracking(closeable)

    val i = new CloseableIterator(Iterator(1), closer)

    val (i0, i1) = i.span(_ => true)

    drainHasNext(i0)

    assert(closeable.isClosed)
    assert(closer.isClosed)
    assert(i0.closer.isClosed)
    assert(i1.closer.isClosed)
  }

  test("span iterator, right has everything") {
    val closeable = new CheckableCloser

    val closer = CloseableIterator.SingleCloseTracking(closeable)

    val i = new CloseableIterator(Iterator(1), closer)

    val (i0, i1) = i.span(_ => false)

    drainHasNext(i0)

    assert(!closeable.isClosed)
    assert(!closer.isClosed)
    assert(!i0.closer.isClosed)
    assert(!i1.closer.isClosed)

    drainHasNext(i1)

    assert(closeable.isClosed)
    assert(closer.isClosed)
    assert(i0.closer.isClosed)
    assert(i1.closer.isClosed)
  }

  test("span iterator, both have something") {
    val closeable = new CheckableCloser

    val closer = CloseableIterator.SingleCloseTracking(closeable)

    val i = new CloseableIterator(Iterator(1,2), closer)

    val (i0, i1) = i.span(_ < 2)

    drainHasNext(i0)
    assert(!closeable.isClosed)
    assert(!i0.closer.isClosed)
    assert(!i1.closer.isClosed)

    drainHasNext(i1)

    assert(closeable.isClosed)
    assert(closer.isClosed)
    assert(i0.closer.isClosed)
    assert(i1.closer.isClosed)
  }

  test("foreach") {
    val closeable = new CheckableCloser

    val closer = CloseableIterator.SingleCloseTracking(closeable)

    val it = new CloseableIterator(Iterator.tabulate(3)(identity), closer)

    var ints = collection.mutable.Set.empty[Int]

    for (i <- it) {
      ints += i
    }

    assert(closeable.isClosed)
    assertResult(0 until 3 toSet)(ints)
  }

  test("filter") {
    val closeable = new CheckableCloser

    val closer = CloseableIterator.SingleCloseTracking(closeable)

    val it = new CloseableIterator(Iterator.tabulate(3)(identity), closer)

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
}
