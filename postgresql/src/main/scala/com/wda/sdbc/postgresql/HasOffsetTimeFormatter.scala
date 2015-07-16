package com.wda.sdbc.postgresql

import java.time.format.{DateTimeFormatterBuilder, DateTimeFormatter}

import com.wda.sdbc.jdbc

trait HasOffsetTimeFormatter
  extends jdbc.HasOffsetTimeFormatter {

  override val offsetTimeFormatter: DateTimeFormatter = {
    new DateTimeFormatterBuilder().
    parseCaseInsensitive().
    append(DateTimeFormatter.ISO_LOCAL_TIME).
    optionalStart().
    appendOffset("+HH:mm", "+00").
    optionalEnd().
    toFormatter
  }

}