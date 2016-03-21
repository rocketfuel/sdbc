package com.rocketfuel.sdbc.base.jdbc

import shapeless.record.Record
import shapeless.{::, HNil}

trait TypesForTesting {
  case class Woozle(a: (String, Int), b: Int :: String :: HNil, c: Boolean)

  type DL = (Int, String)

  type R = Record.`'foo -> Int, 'bar -> String, 'baz -> DL, 'quz -> Woozle`.T

}
