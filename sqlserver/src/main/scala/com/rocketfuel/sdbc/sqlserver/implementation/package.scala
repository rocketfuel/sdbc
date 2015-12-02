package com.rocketfuel.sdbc.sqlserver

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}

package object implementation {

  private[implementation] val offsetDateTimeFormatter = {
    new DateTimeFormatterBuilder().
      parseCaseInsensitive().
      append(DateTimeFormatter.ISO_LOCAL_DATE).
      appendLiteral(' ').
      append(DateTimeFormatter.ISO_LOCAL_TIME).
      optionalStart().
      appendLiteral(' ').
      appendOffset("+HH:MM", "+00:00").
      optionalEnd().
      toFormatter
  }



}
