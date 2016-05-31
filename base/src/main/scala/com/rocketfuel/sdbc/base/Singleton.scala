package com.rocketfuel.sdbc.base

/**
  * Indicates that you only want one row from the DBMS.
  * @param get
  * @tparam A
  */
case class Singleton[A](
  get: A
) extends IndexedSeq[A] {

  override def apply(idx: Int): A =
    idx match {
      case 0 =>
        get
      case _ =>
        throw new IndexOutOfBoundsException()
    }

  override def length: Int = 1

}
