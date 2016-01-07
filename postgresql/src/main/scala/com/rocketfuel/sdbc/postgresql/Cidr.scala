package com.rocketfuel.sdbc.postgresql

import java.net.InetAddress
import org.postgresql.util.PGobject

class Cidr(
  private var cidr: Option[(String, String)]
) extends PGobject() {

  def this() {
    this(None)
  }

  setType("cidr")

  override def getValue: String = {
    val cidrString =
      cidr.map {
        case (ip, netmask) =>
          ip + "/" + netmask
      }

    cidrString.
      getOrElse(throw new IllegalStateException("setValue must be called first"))
  }

  override def setValue(value: String): Unit = {
    for (v <- Option(value)) {
      val parts = v.split('/')
      val ip = parts(0)
      val netmask = parts(1)
      cidr = Some((ip, netmask))
    }
  }

  def address: InetAddress = {
    InetAddress.getByName(cidr.get._1)
  }

  def netmask: Short = {
    cidr.get._2.toShort
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: Cidr =>
        other.cidr == this.cidr
      case _ =>
        false
    }
  }

  override def hashCode(): Int = {
    cidr.hashCode
  }

}

object Cidr {
  def apply(address: String, netmask: String): Cidr = {
    val c = new Cidr(cidr = Some((address, netmask)))
    c
  }

  def unapply(cidr: Cidr): Option[(String, String)] = cidr.cidr
}
