package com.rocketfuel.sdbc.base

import java.io.Closeable

trait CloseableIterator[+A]
  extends Iterator[A]
  with Closeable {

  def mapCloseable[B](f: A => B): CloseableIterator[B] = {
    CloseableIterator[B](
      underlying = map(f),
      close = this.close
    )
  }

}

object CloseableIterator {

  def apply[A](underlying: Iterator[A], close: () => Unit): CloseableIterator[A] =
    new CloseableIterator[A] {
      override def hasNext: Boolean = underlying.hasNext

      override def next(): A = underlying.next()

      override def close(): Unit = close()
    }

}
