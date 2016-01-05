package com.rocketfuel.sdbc.postgresql

import org.postgresql.util.PGobject

class Cidr(
  var cidr: Option[(String, String)] = None
) extends PGobject() {

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
