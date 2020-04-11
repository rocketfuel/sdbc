package com.example.jsondbms

import argonaut.Argonaut._
import argonaut._
import cats.effect.{Async, Blocker, ExitCode, IO, IOApp}
import cats.implicits._
import com.example.jsondbms.syntax._
import fs2._

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

object Main extends IOApp {

  def printPirates[F[_]](implicit a: Async[F]): Pipe[F, Pirate, Byte] =
    (pirates: Stream[F, Pirate]) =>
      for {
        pirate <- pirates
        pirateBytes = pirate.toString.getBytes :+ '\n'.toByte
        byte <- Stream(pirateBytes.toIndexedSeq: _*)
      } yield byte

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

  val inserts: Stream[IO, Unit] =
    Stream[IO, Pirate](pirates.toSeq: _*).through(Insert.sink)

  val hadesPearlCrew: Stream[IO, Unit] = Stream.resource(Blocker[IO]).flatMap { blocker =>
    hadesPearl.stream[IO, Pirate].through(printPirates).through(fs2.io.stdout[IO](blocker))
  }

  val job = inserts ++ hadesPearlCrew

  override def run(args: List[String]): IO[ExitCode] = {
    job.compile.drain.as(ExitCode.Success)
  }
}
