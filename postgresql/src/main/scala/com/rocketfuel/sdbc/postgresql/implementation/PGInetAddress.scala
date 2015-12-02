package com.rocketfuel.sdbc.postgresql.implementation

import java.net.InetAddress
import java.sql.SQLException

import org.postgresql.util.PGobject

private[sdbc] class PGInetAddress() extends PGobject() {

  setType("inet")

  var inetAddress: Option[InetAddress] = None

  override def getValue: String = {
    inetAddress.map(_.getHostAddress).
      getOrElse(throw new IllegalStateException("setValue must be called first"))
  }

  override def setValue(value: String): Unit = {
    inetAddress = Option(value).map(InetAddress.getByName)
  }

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case pg: PGInetAddress =>
        pg.inetAddress == this.inetAddress
      case _ => false
    }
  }

  override def hashCode(): Int = {
    inetAddress.hashCode()
  }
}

private[sdbc] object PGInetAddress {
  def apply(address: InetAddress): PGInetAddress = {
    val a = new PGInetAddress()
    a.inetAddress = Some(address)
    a
  }
}

private[sdbc] trait PGInetAddressImplicits {
  implicit def InetAddressToPGobject(x: InetAddress): PGobject = {
    PGInetAddress(x)
  }

  implicit def PGobjectToInetAddress(x: PGobject): InetAddress = {
    x match {
      case p: PGInetAddress =>
        p.inetAddress.get
      case _ =>
        throw new SQLException("column does not contain an inet")
    }
  }
}
