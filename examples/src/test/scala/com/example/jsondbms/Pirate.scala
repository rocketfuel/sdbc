package com.example.jsondbms

import argonaut._
import argonaut.Argonaut._
import fs2._
import fs2.util.Async
import scala.concurrent.ExecutionContext

case class Pirate(
  ship: String,
  name: String,
  battleCry: String,
  shoulderPet: String
)

object Pirate {
  implicit val PirateCodecJson: CodecJson[Pirate] =
    casecodec4(Pirate.apply, Pirate.unapply)("ship", "name", "battleCry", "shoulderPet")
}

case class Ship(
  ship: String
)

object Ship {
  implicit val ShipCodecJson: CodecJson[Ship] =
    casecodec1(Ship.apply, Ship.unapply)("ship")
}

object Main {

  def pirateLines[F[_]](implicit a: Async[F]): Pipe[F, Pirate, Byte] =
    (pirates: Stream[F, Pirate]) =>
      for {
        pirate <- pirates
        byte <- Stream(pirate.toString.getBytes :+ '\n'.toByte: _*)
      } yield byte

  def main(args: Seq[String]): Unit = {
    implicit val strategy = Strategy.fromExecutionContext(ExecutionContext.global)

    //Thanks to http://gangstaname.com/names/pirate and http://www.seventhsanctum.com/generate.php?Genname=pirateshipnamer
    val pirates =
      Set(
        Pirate("Killer's Fall", "Fartin' Garrick Hellion", "yar", "Cap'n Laura Cannonballs"),
        Pirate("Pirate's Shameful Poison", "Pirate Ann Marie the Well-Tanned", "arrr", "Cheatin' Louise Bonny")
      )

    implicit val pool: ConnectionPool = ???

    val inserts: Stream[Task, Unit] =
      Stream(pirates.toSeq: _*).covary[Task].to(Insert.sink)

    val killersFall = Ship("Killer's Fall")

    val killersFallCrewMembers: Stream[Task, Pirate] =
      Select.stream[Task, Ship, Pirate](killersFall)

    //run the insert
    inserts.run.unsafeRun()

    //run select and print the pirates
    killersFallCrewMembers.through(pirateLines).to(fs2.io.stdout).run.unsafeRun()

    //demonstrate cool syntax
    import syntax._
    implicit val connection: Connection = ???
    pirates.foreach(_.insert())
    killersFall.iterator[Pirate]()
  }
  //TODO: Copy to DIALECT.md

}
