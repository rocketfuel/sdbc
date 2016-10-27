package com.rocketfuel.sdbc.postgresql

import java.net.InetAddress
import org.scalatest.FunSuite

class CidrSpec extends FunSuite {

  test("valueOf and toString are inverses") {
    val cidr = Cidr(InetAddress.getByName("0.0.0.0"), 0)
    assertResult(cidr)(Cidr.valueOf(cidr.toString))
  }

  test("toString") {
    assertResult("255.255.255.255/65535")(Cidr(InetAddress.getByName("255.255.255.255"), 65535).toString)
  }

}
