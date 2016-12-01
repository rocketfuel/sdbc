package com.rocketfuel.sdbc

import java.time.ZoneOffset
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}

package object sqlserver {
  val offsetDateTimeFormatter: DateTimeFormatter =
    new DateTimeFormatterBuilder().
      parseCaseInsensitive().
      append(DateTimeFormatter.ISO_LOCAL_DATE).
      appendLiteral(' ').
      append(DateTimeFormatter.ISO_LOCAL_TIME).
      optionalStart().
      appendLiteral(' ').
      appendOffset("+HH:MM", "+00:00").
      optionalEnd().
      toFormatter()

  val instantFormatter: DateTimeFormatter =
    offsetDateTimeFormatter.
      withZone(ZoneOffset.UTC)
}
