package com.rocketfuel.sdbc.postgresql

import java.net.InetAddress

import com.rocketfuel.sdbc.postgresql.implementation.PGInetAddress

class Cidr() extends PGInetAddress() {

  setType("cidr")

  var netmask: Option[Short] = None

  override def getValue: String = {
    val result = for {
      inetaddr <- Option(super.getValue)
      actualMask <- netmask
    } yield {
      inetaddr + '/' + actualMask.toString
    }

    result.
      getOrElse(throw new IllegalStateException("setValue must be called first"))
  }

  override def setValue(value: String): Unit = {
    val parts = value.split('/')
    super.setValue(parts(0))
    netmask = Some(parts(1).toShort)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: Cidr =>
        other.inetAddress == this.inetAddress &&
          other.netmask == this.netmask
      case _ =>
        false
    }
  }

  override def hashCode(): Int = {
    super.hashCode() ^ netmask.hashCode()
  }

}

object Cidr {
  def apply(address: InetAddress, netmask: Short): Cidr = {
    val c = new Cidr()
    c.inetAddress = Some(address)
    c.netmask = Some(netmask)
    c
  }
}
