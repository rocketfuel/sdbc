package com.rocketfuel.sdbc.h2.jdbc.implementation

import com.rocketfuel.sdbc.base.jdbc._

private[sdbc] trait Getters
  extends DefaultGetters
  with LocalDateGetter
  with LocalDateTimeGetter
  with LocalTimeGetter
  with InstantGetter
