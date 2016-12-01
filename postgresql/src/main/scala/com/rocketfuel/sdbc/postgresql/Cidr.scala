package com.rocketfuel.sdbc.postgresql

import java.net.InetAddress
import org.postgresql.util.PGobject

/**
  * Corresponds to PostgreSql's [[https://www.postgresql.org/docs/current/static/datatype-net-types.html cidr]] type.
  */
class Cidr private (
  private var memo: Option[Cidr.Memo]
) extends PGobject() {

  /**
    * Use [[Cidr.apply]] or [[Cidr.valueOf]] instead. This constructor is for use by the PostgreSql driver.
    */
  def this() {
    this(None)
  }

  setType("cidr")

  private def mustSetValue =
    throw new IllegalStateException("setValue must be called first")

  private def getMemo: Cidr.Memo =
    memo.getOrElse(mustSetValue)

  override def getValue: String = {
    getMemo.value
  }

  override def setValue(value: String): Unit = {
    if (memo.isDefined)
      throw new IllegalStateException("setValue can not be called twice on the same instance")

    memo = Option(value).map(Cidr.Memo.valueOf)
  }

  def address: InetAddress = {
    getMemo.address
  }

  def netmask: Int = {
    getMemo.netmask
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case Cidr(otherAddress, otherNetmask) =>
        otherAddress == address &&
          otherNetmask == netmask
      case _ =>
        false
    }
  }

  override def toString: String = {
    getMemo.value
  }

  override def hashCode(): Int = {
    getMemo.value.hashCode
  }

}

object Cidr {
  def apply(address: InetAddress, netmask: Int): Cidr = {
    val c = new Cidr(Some(Memo(address, netmask, address.getHostAddress + "/" + netmask)))
    c
  }

  def unapply(cidr: Cidr): Option[(InetAddress, Int)] = cidr.memo.map(c => (c.address, c.netmask))

  def valueOf(value: String): Cidr = {
    new Cidr(Some(Memo.valueOf(value)))
  }

  private case class Memo(
    address: InetAddress,
    netmask: Int,
    value: String
  )

  private object Memo {
    def valueOf(value: String): Memo = {
      val parts = value.split('/')
      val ip = InetAddress.getByName(parts(0))
      val netmask = parts(1).toInt
      new Memo(ip, netmask, value)
    }
  }
}
