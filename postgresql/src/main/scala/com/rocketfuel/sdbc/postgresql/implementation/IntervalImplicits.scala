package com.rocketfuel.sdbc.postgresql.implementation

import java.sql.SQLException
import java.time.{Duration => JavaDuration}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

import org.postgresql.util.{PGobject, PGInterval}

private[sdbc] trait IntervalImplicits {

  implicit def JavaDurationToPGInterval(value: JavaDuration): PGInterval = {
    val nano = value.getNano.toDouble / IntervalConstants.nanosecondsPerSecond.toDouble
    val totalSeconds = value.getSeconds
    val years = totalSeconds / IntervalConstants.secondsPerYear
    val yearRemainder = totalSeconds % IntervalConstants.secondsPerYear
    val months = yearRemainder / IntervalConstants.secondsPerMonth
    val monthRemainder = yearRemainder % IntervalConstants.secondsPerMonth
    val days = monthRemainder / IntervalConstants.secondsPerDay
    val dayRemainder = monthRemainder % IntervalConstants.secondsPerDay
    val hours = dayRemainder / IntervalConstants.secondsPerHour
    val hoursRemainder = dayRemainder % IntervalConstants.secondsPerHour
    val minutes = hoursRemainder / IntervalConstants.secondsPerMinute
    val seconds = (hoursRemainder % IntervalConstants.secondsPerMinute).toDouble + nano
    new PGInterval(
      years.toInt,
      months.toInt,
      days.toInt,
      hours.toInt,
      minutes.toInt,
      seconds
    )
  }

  implicit def PGIntervalToJavaDuration(value: PGInterval): JavaDuration = {
    val nanos = (value.getSeconds - value.getSeconds.floor) * IntervalConstants.nanosecondsPerSecond
    var seconds = 0L
    seconds += value.getSeconds.toLong
    seconds += value.getMinutes * IntervalConstants.secondsPerMinute
    seconds += value.getHours * IntervalConstants.secondsPerHour
    seconds += value.getDays * IntervalConstants.secondsPerDay
    seconds += value.getMonths * IntervalConstants.secondsPerMonth
    seconds += value.getYears * IntervalConstants.secondsPerYear
    JavaDuration.ofSeconds(seconds, nanos.toLong)
  }

  implicit def DurationToPGInterval(duration: Duration): PGInterval = {
    val javaDuration = JavaDuration.ofNanos(duration.toNanos)
    javaDuration
  }

  implicit def PGIntervalToDuration(value: PGInterval): Duration = {
    val javaDuration: JavaDuration = value
    Duration(javaDuration.getSeconds, TimeUnit.SECONDS) + Duration(javaDuration.getNano, TimeUnit.NANOSECONDS)
  }

  implicit def PGobjectToScalaDuration(value: PGobject): Duration = {
    value match {
      case p: PGInterval => p
      case _ =>
        throw new SQLException("column does not contain a interval")
    }
  }

  implicit def PGobjectToJavaDuration(value: PGobject): JavaDuration = {
    value match {
      case p: PGInterval => p
      case _ =>
        throw new SQLException("column does not contain a interval")
    }
  }

}
