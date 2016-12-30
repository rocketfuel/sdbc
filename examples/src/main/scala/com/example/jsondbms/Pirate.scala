package com.example.jsondbms

import argonaut.Argonaut._
import argonaut._
import com.example.jsondbms.syntax._
import fs2._
import fs2.util.Async
import scala.concurrent.ExecutionContext

case class Pirate(
  ship: String,
  name: String,
  shoulderPet: String //every pirate needs some animal on his or her shoulder
)

object Pirate {
  implicit val PirateCodecJson: CodecJson[Pirate] =
    casecodec3(Pirate.apply, Pirate.unapply)("ship", "name", "shoulderPet")
}

case class Ship(
  ship: String
)

object Ship {
  implicit val ShipCodecJson: CodecJson[Ship] =
    casecodec1(Ship.apply, Ship.unapply)("ship")
}

object Main {

  def printPirates[F[_]](implicit a: Async[F]): Pipe[F, Pirate, Byte] =
    (pirates: Stream[F, Pirate]) =>
      for {
        pirate <- pirates
        byte <- Stream(pirate.toString.getBytes :+ '\n'.toByte: _*)
      } yield byte

  def main(args: Array[String]): Unit = {
    implicit val strategy = Strategy.fromExecutionContext(ExecutionContext.global)

    //Thanks http://www.seventhsanctum.com/generate.php?Genname=pirateshipnamer
    val hadesPearl = Ship("Hades' Pearl")
    val oceansEvilPoison = Ship("Ocean's Evil Poison")

    //Thanks to http://gangstaname.com/names/pirate
    val pirates =
      Set(
        Pirate(hadesPearl.ship, "Fartin' Garrick Hellion", "Cap'n Laura Cannonballs"),
        Pirate(hadesPearl.ship, "Pirate Ann Marie the Well-Tanned", "Cheatin' Louise Bonny"),
        Pirate(oceansEvilPoison.ship, "Fish Breath Rupert", "Rancid Dick Scabb")
      )

    implicit val db: JsonDb = new JsonDb

    val inserts: Stream[Task, Unit] =
      Stream[Task, Pirate](pirates.toSeq: _*).to(Insert.sink)

    val hadesPearlCrew =
      hadesPearl.stream[Task, Pirate].through(printPirates).to(fs2.io.stdout)

    Seq(inserts, hadesPearlCrew).foreach(_.run.unsafeRun())
  }

}
