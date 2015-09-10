package com.rocketfuel.sdbc.h2.jdbc.implementation

import com.rocketfuel.sdbc.base.jdbc._

trait Setters
  extends DefaultSetters
  with QInstantImplicits
  with QLocalDateImplicits
  with QLocalTimeImplicits
  with QLocalDateTimeImplicits {

  val toH2Parameter: PartialFunction[Any, Any] =
    toDefaultParameter orElse
      QInstant.toParameter orElse
      QLocalDate.toParameter orElse
      QLocalTime.toParameter orElse
      QLocalDateTime.toParameter orElse
      QSeq.toParameter

}
