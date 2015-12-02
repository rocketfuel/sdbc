package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core
import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.cassandra.Select
import scala.concurrent.{ExecutionContext, Future}

private[sdbc] trait SelectableMethods extends base.SelectableMethods[core.Session, Select] {

  def iteratorAsync[Key, Value](
    key: Key
  )(implicit ev: Selectable[Key, Value],
    connection: Session,
    ec: ExecutionContext
  ): Future[Iterator[Value]] = {
    ev.select(key).iteratorAsync()
  }

}
