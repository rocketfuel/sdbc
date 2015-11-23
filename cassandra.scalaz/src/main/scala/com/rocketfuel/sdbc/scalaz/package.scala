package com.rocketfuel.sdbc

import _root_.scalaz.stream.Process

package object scalaz {

  object HasDatastax {
    val datastax = new datastax {}
  }

  implicit def ProcessToDatastaxProcess(x: Process.type): HasDatastax.type = {
    HasDatastax
  }

}
