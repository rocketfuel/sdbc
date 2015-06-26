package com.wda.sdbc
package postgresql

import PostgreSql._
import IntervalConstants._
import org.joda.time.Duration
import org.postgresql.util.PGInterval
import org.scalatest.FunSuite

class PGIntervalSpec extends FunSuite {

  val str = "9 years 11 mons 29 days 06:41:38.636266"

  val asPg = new PGInterval(str)

  val asPgParts = new PGInterval(9, 11, 29, 6, 41, 38.636266)

  val asDuration: Duration = asPg

  test("PGInterval created from string equals PGInterval created from parts") {
    assert(asPg.getValue == asPgParts.getValue)
  }

  test("Duration converted from PGInterval equals Duration") {
    val millis = 636L
    var seconds = 0L
    seconds += 38
    seconds += 41 * secondsPerMinute
    seconds += 6 * secondsPerHour
    seconds += 29 * secondsPerDay
    seconds += 11 * secondsPerMonth
    seconds += 9 * secondsPerYear
    val duration = new Duration(seconds * 1000 + millis)

    val difference = asDuration.minus(duration)
    val differenceNanos = difference.getSeconds * millisecondsPerSecond + difference.getMillis

    assert(differenceNanos.abs < 100, "The difference must be less than 100 nanoseconds.")
  }

  test("PGInterval <-> Duration conversion is commutative.") {
    val asPg2: PGInterval = asDuration

    assert(asPg.getYears == asPg2.getYears)
    assert(asPg.getMonths == asPg2.getMonths)
    assert(asPg.getDays == asPg2.getDays)
    assert(asPg.getHours == asPg2.getHours)
    assert(asPg.getMinutes == asPg2.getMinutes)
    assert((asPg.getSeconds - asPg2.getSeconds).abs < 0.001,
      "The difference between the seconds is larger than we allow for rounding."
    )
  }

}
