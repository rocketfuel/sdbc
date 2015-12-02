package com.rocketfuel.sdbc.postgresql.implementation

private object IntervalConstants {
  val nanosecondsPerSecond = 1000000000L

  //Constants from PostgreSQL's source: src/include/datatype/timestamp.h
  val secondsPerMinute = 60
  val secondsPerHour = 3600
  val daysPerYear = 365.25
  val secondsPerDay = 86400
  val daysPerMonth = 30

  //31557600
  val secondsPerYear = (daysPerYear * secondsPerDay).toInt

  //2592000
  val secondsPerMonth = daysPerMonth * secondsPerDay
}
